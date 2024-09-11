#region License
// Copyright (c) 2016 Timothy Slater
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
#endregion

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using TraceWizard.Data;

namespace TraceWizard.Processors
{
    public class SQLProcessor : ITraceProcessor
    {
        public List<SQLStatement> Statements;
        private SQLStatement currentStatement;

        Regex contextMarker = new Regex("PSAPPSRV\\.\\d+ \\(\\d+\\)");
        Regex lineValid = new Regex("(COM Stmt=)|(Bind-\\d+)|(Fetch)|(EXE)|(EPO)|(ERR)");
        Regex newStatement = new Regex("RC=(\\d+) Dur=(\\d+\\.\\d+) COM Stmt=(.*)");
        Regex execStatement = new Regex("RC=(\\d+) Dur=(\\d+\\.\\d+) EXE");
        Regex fetchStatement = new Regex("RC=(\\d+) Dur=(\\d+\\.\\d+) Fetch");
        Regex bindStatement = new Regex("Bind-(\\d+) type=(\\d+) length=(\\d+) value=(.*)");
        Regex errorLine = new Regex("ERR rtncd=(\\d+) msg=(.*)");
        Regex errPosStatement = new Regex("EPO error pos=(\\d+)");

        public void ProcessLine(string line, long lineNumber)
        {
            if (lineValid.IsMatch(line) == false)
            {
                return;
            }


            Match m = newStatement.Match(line);
            if (m.Success)
            {
                if (currentStatement != null)
                {
                    Statements.Add(currentStatement);
                }
                string context = contextMarker.Match(line).Groups[0].Value;

                currentStatement = new SQLStatement(m.Groups[3].Value);
                currentStatement.Context = context;
                currentStatement.RCNumber = int.Parse(m.Groups[1].Value);
                currentStatement.LineNumber = lineNumber;

                /* AET TRC files don't have explicit Exec statements, so set the Exec time from the COM Stmt line's Duration */
                /* this will be overwritten in case we are in a tracesql file by the subsequent Exec statement */
                currentStatement.ExecTime = double.Parse(m.Groups[2].Value);
                return;
            }

            m = execStatement.Match(line);
            if (m.Success)
            {
                currentStatement.ExecTime = double.Parse(m.Groups[2].Value);
                return;
            }

            m = fetchStatement.Match(line);
            if (m.Success)
            {
                if (int.Parse(m.Groups[1].Value) != currentStatement.RCNumber)
                {
                    return;
                }

                currentStatement.FetchCount++;
                currentStatement.FetchTime += double.Parse(m.Groups[2].Value);
                return;
            }

            m = bindStatement.Match(line);
            if (m.Success)
            {
                SQLBindValue bind = new SQLBindValue();
                bind.Index = int.Parse(m.Groups[1].Value);
                bind.Type = int.Parse(m.Groups[2].Value);
                bind.Length = int.Parse(m.Groups[3].Value);
                bind.Value = m.Groups[4].Value;
                currentStatement.BindValues.Add(bind);
                return;
            }

            m = errPosStatement.Match(line);
            if (m.Success)
            {
                /* we have an error on this SQL */
                currentStatement.IsError = true;
                currentStatement.ErrorInfo = new SQLError();

                currentStatement.ErrorInfo.ErrorPosition = Int32.Parse(m.Groups[1].Value);
                return;
            }

            m = errorLine.Match(line);
            if (m.Success)
            {
                currentStatement.ErrorInfo.ReturnCode = Int32.Parse(m.Groups[1].Value);
                currentStatement.ErrorInfo.Message = m.Groups[2].Value;
            }
        }

        public void ProcessorInit(TraceData data)
        {
            Statements = data.SQLStatements;
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
        }

        public void ProcessorComplete(TraceData td)
        {
            if ((Statements.Count == 0) && currentStatement == null) {
                return;
            }
            /* add final sql statement */
            if (currentStatement != null)
            {
                Statements.Add(currentStatement);
            }

            /* Group them all by Where */
            var sqlByWhereList = td.SQLByWhere;

            var byWheres = Statements.Where(p => p.Type != SQLType.INSERT).GroupBy(p => p.WhereClause).Select( g => new SQLByWhere{ NumberOfCalls = g.Count(), TotalTime = g.Sum(i => i.Duration), WhereClause = g.Key, HasError = g.Count(p => p.IsError) > 0 ? true : false});
            foreach (var byW in byWheres)
            {
                sqlByWhereList.Add(byW);
            }

            var sqlByFromList = td.SQLByFrom;
            var byFroms = Statements.Where(p => p.Type == SQLType.SELECT || p.Type == SQLType.DELETE).GroupBy(p => p.FromClause).Select(g => new SQLByFrom{ NumberOfCalls = g.Count(), TotalTime = g.Sum(i => i.Duration), FromClause = g.Key, HasError = g.Count(p => p.IsError) > 0 ? true : false });
            foreach (var byF in byFroms)
            {
                sqlByFromList.Add(byF);
            }

            td.Statistics.Add(new StatisticItem() { Category = "SQL Statements", Label = "Total Count", Value = Statements.Count.ToString() });
            SQLStatement longest = Statements.OrderBy(s => s.Duration).Reverse().First();
            td.Statistics.Add(new StatisticItem() { Category = "SQL Statements", Label = "Longest Execution", Value = longest.Duration.ToString(),Tag = longest });
            SQLStatement mostFetches = Statements.OrderBy(s => s.FetchCount).Reverse().First();
            td.Statistics.Add(new StatisticItem() { Category = "SQL Statements", Label = "Most Fetches", Value = mostFetches.FetchCount.ToString(), Tag = mostFetches });
            td.Statistics.Add(new StatisticItem(){Category = "SQL Statements", Label = "Total SQL Time", Value = Statements.Sum(s => s.Duration).ToString()});
            
            td.Statistics.Add(new StatisticItem(){Category = "SQL Statements", Label = "Total SELECT Time", Value = Statements.Where(s => s.Type == SQLType.SELECT).Sum(s => s.Duration).ToString()});
            td.Statistics.Add(new StatisticItem(){Category = "SQL Statements", Label = "Total UPDATE Time", Value = Statements.Where(s => s.Type == SQLType.UPDATE).Sum(s => s.Duration).ToString()});
            td.Statistics.Add(new StatisticItem(){Category = "SQL Statements", Label = "Total INSERT Time", Value = Statements.Where(s => s.Type == SQLType.INSERT).Sum(s => s.Duration).ToString()});
            td.Statistics.Add(new StatisticItem(){Category = "SQL Statements", Label = "Total DELETE Time", Value = Statements.Where(s => s.Type == SQLType.DELETE).Sum(s => s.Duration).ToString()});
        }
    }
}

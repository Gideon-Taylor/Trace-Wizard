using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using TraceWizard.Data;

namespace TraceWizard.Processors
{
    internal class AETSQLProcessor : ITraceProcessor
    {
        bool expectingSQL = false;
        bool expectingBuffers = false;
        bool expectingBinds = false;
        StringBuilder currentSQL = new StringBuilder();
        SQLStatement latestSQLStatement = null;
        long sqlStartLine = 0;
        Regex stepHasSQL = new Regex(@"\(Do When\)$|\(Do While\)$|\(Do Select\)$|\(SQL\)$|\(Do Until\)$");
        Regex bufferBindValue = new Regex(@"--\s+(\d+)\) (.*)");
        List<SQLStatement> statements = null;

        public void ProcessLine(string line, long lineNumber)
        {
            if (stepHasSQL.IsMatch(line))
            {
                expectingSQL = true;
                return;
            }

            if (line.Equals("-- Buffers:"))
            {
                expectingBuffers = true;
                return;
            }

            if (line.Equals("-- Bind variables:"))
            {
                expectingBinds = true;
                return;
            }

            if (expectingBuffers)
            {
                var match = bufferBindValue.Match(line);
                if (match.Success)
                {
                    /* add to buffer of previous SQL */
                    if (latestSQLStatement.BufferData == null)
                    {
                        /* init buffer data */
                        latestSQLStatement.BufferData = new List<string>();
                    }
                    latestSQLStatement.BufferData.Add(match.Groups[2].Value);
                } else
                {
                    expectingBuffers = false;
                    return;
                }
            }

            if (expectingBinds)
            {
                var match = bufferBindValue.Match(line);
                if (match.Success)
                {
                    SQLBindValue bind = new SQLBindValue();
                    bind.Index = int.Parse(match.Groups[1].Value);
                    bind.Type = 0;
                    bind.Length = match.Groups[2].Value.Length;
                    bind.Value = match.Groups[2].Value;

                    latestSQLStatement.AddBindValue(bind);
                }
                else
                {
                    expectingBinds = false;
                    return;
                }
            } else
            {
                            }


            if (expectingSQL && line.Equals("/") == false)
            {
                if (sqlStartLine == 0)
                {
                    sqlStartLine = lineNumber;
                }

                /* dirty check for if we might be in a loop or something and we dont have a SQL where we expect it */
                if (line.StartsWith("--"))
                {
                    /* this isn't where we want to be... false positive for SQL statement */
                    sqlStartLine = 0;
                    currentSQL.Clear();
                    expectingSQL = false;
                    return;
                }

                currentSQL.Append(line);
                return;
            }

            if (expectingSQL && line.Equals("/"))
            {

                latestSQLStatement = new SQLStatement(currentSQL.ToString());
                latestSQLStatement.LineNumber = sqlStartLine;

                statements.Add(latestSQLStatement);

                /* reset state */
                sqlStartLine = 0;
                currentSQL.Clear();
                expectingSQL = false;

            }

            //throw new System.NotImplementedException();
        }

        public void ProcessorComplete(TraceData data)
        {
            //throw new System.NotImplementedException();
            if (statements.Count == 0)
            {
                return;
            }

            /* Group them all by Where */
            var sqlByWhereList = data.SQLByWhere;

            var byWheres = statements.Where(p => p.Type != SQLType.INSERT).GroupBy(p => p.WhereClause).Select(g => new SQLByWhere { NumberOfCalls = g.Count(), TotalTime = g.Sum(i => i.Duration), WhereClause = g.Key, HasError = g.Count(p => p.IsError) > 0 ? true : false });
            foreach (var byW in byWheres)
            {
                sqlByWhereList.Add(byW);
            }

            var sqlByFromList = data.SQLByFrom;
            var byFroms = statements.Where(p => p.Type == SQLType.SELECT || p.Type == SQLType.DELETE).GroupBy(p => p.FromClause).Select(g => new SQLByFrom { NumberOfCalls = g.Count(), TotalTime = g.Sum(i => i.Duration), FromClause = g.Key, HasError = g.Count(p => p.IsError) > 0 ? true : false });
            foreach (var byF in byFroms)
            {
                sqlByFromList.Add(byF);
            }

            data.Statistics.Add(new StatisticItem() { Category = "SQL Statements", Label = "Total Count", Value = statements.Count.ToString() });
            SQLStatement longest = statements.OrderBy(s => s.Duration).Reverse().First();
            data.Statistics.Add(new StatisticItem() { Category = "SQL Statements", Label = "Longest Execution", Value = longest.Duration.ToString(), Tag = longest });
            SQLStatement mostFetches = statements.OrderBy(s => s.FetchCount).Reverse().First();
            data.Statistics.Add(new StatisticItem() { Category = "SQL Statements", Label = "Most Fetches", Value = mostFetches.FetchCount.ToString(), Tag = mostFetches });
        }

        public void ProcessorInit(TraceData data)
        {
            statements = data.SQLStatements;
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
        }
    }
}
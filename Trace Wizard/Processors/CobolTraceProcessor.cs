using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using TraceWizard.Data;

namespace TraceWizard.Processors
{
    class CobolTraceProcessor : ITraceProcessor
    {
        public List<SQLStatement> Statements;
        private SQLStatement currentStatement;

        Regex lineValid = new Regex("(CEX Stmt=)|(COM Stmt=)|(Bind position=\\d+)|(Fetch)|(EXE)|(EPO)|(ERR)");
        Regex newStatement = new Regex("RC=(\\d+)\\s+(COM|CEX) Stmt=(.*)");
        Regex execStatement = new Regex("RC=(\\d+)\\s+EXE");
        Regex fetchStatement = new Regex("RC=(\\d+)\\s+Fetch");
        Regex bindStatement = new Regex("Bind position=(\\d+), type=(.+), length=(\\d+), value=(.*)");
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

                currentStatement = new SQLStatement(m.Groups[3].Value);
                currentStatement.Context = "COBOL";
                currentStatement.RCNumber = int.Parse(m.Groups[1].Value);
                currentStatement.Cobol = true;
                /* TODO: Pull line number from COBOL trace, use that instead of File line number */
                currentStatement.LineNumber = lineNumber;
                return;
            }

/* TODO: Determine if there is a meaningful value to return for EXE statements */
            /* m = execStatement.Match(line);
            if (m.Success)
            {
                currentStatement.ExecTime = double.Parse(m.Groups[2].Value);
                return;
            }*/

            m = fetchStatement.Match(line);
            if (m.Success)
            {
                if (int.Parse(m.Groups[1].Value) != currentStatement.RCNumber)
                {
                    return;
                }

                currentStatement.FetchCount++;
                /* TODO: Pull fetch time? */
                /* currentStatement.FetchTime += double.Parse(m.Groups[2].Value); */
                return;
            }

            m = bindStatement.Match(line);
            if (m.Success)
            {
                SQLBindValue bind = new SQLBindValue();
                bind.Index = int.Parse(m.Groups[1].Value);
                bind.TypeString = m.Groups[2].Value;
                bind.Length = int.Parse(m.Groups[3].Value);
                bind.Value = m.Groups[4].Value;
                currentStatement.BindValues.Add(bind);
                return;
            }

            /* TODO: How do errors appear in Cobol statements? */
            /* m = errPosStatement.Match(line);
            if (m.Success)
            {
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
            }*/
        }

        public void ProcessorInit(TraceData data)
        {
            Statements = data.SQLStatements;
        }

        public void ProcessorComplete(TraceData td)
        {
            if ((Statements.Count == 0) && currentStatement == null)
            {
                return;
            }
            /* add final sql statement */
            Statements.Add(currentStatement);

            /* Group them all by Where */
            var sqlByWhereList = td.SQLByWhere;

            var byWheres = Statements.Where(p => p.Type != SQLType.INSERT).GroupBy(p => p.WhereClause).Select(g => new SQLByWhere { NumberOfCalls = g.Count(), TotalTime = g.Sum(i => i.Duration), WhereClause = g.Key, HasError = g.Count(p => p.IsError) > 0 ? true : false });
            foreach (var byW in byWheres)
            {
                sqlByWhereList.Add(byW);
            }

            var sqlByFromList = td.SQLByFrom;
            var byFroms = Statements.Where(p => p.Type == SQLType.SELECT || p.Type == SQLType.DELETE).GroupBy(p => p.FromClause).Select(g => new SQLByFrom { NumberOfCalls = g.Count(), TotalTime = g.Sum(i => i.Duration), FromClause = g.Key, HasError = g.Count(p => p.IsError) > 0 ? true : false });
            foreach (var byF in byFroms)
            {
                sqlByFromList.Add(byF);
            }

            td.Statistics.Add(new StatisticItem() { Category = "SQL Statements", Label = "Total Count", Value = Statements.Count.ToString() });
            SQLStatement longest = Statements.OrderBy(s => s.Duration).Reverse().First();
            td.Statistics.Add(new StatisticItem() { Category = "SQL Statements", Label = "Longest Execution", Value = longest.Duration.ToString(), Tag = longest });
            SQLStatement mostFetches = Statements.OrderBy(s => s.FetchCount).Reverse().First();
            td.Statistics.Add(new StatisticItem() { Category = "SQL Statements", Label = "Most Fetches", Value = mostFetches.FetchCount.ToString(), Tag = mostFetches });

        }
    }
}

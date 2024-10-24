using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using TraceWizard.Data;

namespace TraceWizard.Processors
{
    class CobolLineHeader
    {
        public string Time;
        public string Line;
        public double Duration;
        public double SQLDuration;
        public int Cursor;
        public int RCNumber;

        public static CobolLineHeader FromLogLine(string logLine)
        {
            // Extract values using Substring based on fixed widths
            string time = logLine.Substring(0, 12).Trim();
            string line = logLine.Substring(14, 9).Trim();
            string elapsedStr = logLine.Substring(26, 7).Trim();
            string sqlTimeStr = logLine.Substring(36, 7).Trim();
            string cursorStr = logLine.Substring(47, 6).Trim();
            string rcStr = logLine.Substring(56, 4).Trim();

            // Convert extracted string values to appropriate data types
            double elapsed = double.Parse(elapsedStr, CultureInfo.InvariantCulture);
            double sqlTime = double.Parse(sqlTimeStr, CultureInfo.InvariantCulture);
            int cursor = int.Parse(cursorStr);
            int rcNumber = int.Parse(rcStr);


            // Create and return the struct instance
            return new CobolLineHeader
            {
                Time = time,
                Line = line,
                Duration = elapsed,
                SQLDuration = sqlTime,
                Cursor = cursor,
                RCNumber = rcNumber
            };
        }
    }
    internal class CobolSQLProcessor : ITraceProcessor
    {
        Dictionary<int, SQLStatement> cursorMap = new Dictionary<int, SQLStatement>();
        const int RTNCD_OK = 0;
        const int RTNCD_END = 1;
        public List<SQLStatement> Statements;
        Regex compileStatement = new Regex("COM Stmt=(.*)");
        Regex compExecStatement = new Regex("CEX Stmt=(.*)");
        Regex execStatement = new Regex("EXE");
        Regex fetchStatement = new Regex("Fetch");
        Regex bindStatement = new Regex(@"(Bind-(\d+)|Bind position=(\d+)), type=(.*?), (precision=(\d+), scale=(\d+)|length=(\d+)), value=(.*)");
        Regex getStatement = new Regex("GETSTMT Stmt=(.*?), length");

        private string pendingStatementID = null;
        
        private static bool IsValid(string line)
        {
            if (line == null)
                return false;

            return line.Contains("COM Stmt=") ||
                   line.Contains("Bind-") ||
                   line.Contains("Bind position") ||
                   line.Contains(" Fetch") ||
                   line.Contains(" EXE") ||
                   line.Contains(" EPO") ||
                   line.Contains(" ERR") ||
                   line.Contains(" CEX Stmt=") ||
                   line.Contains(" GETSTMT Stmt=");
        }
 
        public void ProcessLine(string line, long lineNumber)
        {
            if (CobolSQLProcessor.IsValid(line) == false)
            {
                return;
            }

            var header = CobolLineHeader.FromLogLine(line);
            /* Check for get statement line */
            var m = getStatement.Match(line);
            if (m.Success)
            {
                pendingStatementID = m.Groups[1].Value;
                return;
            }

            /* Check for a new statement */
            var compMatch = compileStatement.Match(line);
            var compExecMatch = compExecStatement.Match(line);
            if (compMatch.Success || compExecMatch.Success)
            {
                if (compMatch.Success)
                {
                    m = compMatch;
                }else
                {
                    m = compExecMatch;
                }

                var currentStatement = new SQLStatement(m.Groups[1].Value);
                //currentStatement.Context = $"Cursor {header.Cursor}";
                currentStatement.Cobol = true;
                currentStatement.Cursor = header.Cursor;
                currentStatement.RCNumber = header.RCNumber;
                currentStatement.LineNumber = lineNumber;

                /* Not exactly sure how COBOL handles durations. the trace file i have has basically all 0's */
                if (compExecMatch.Success)
                {
                    currentStatement.ExecTime = header.SQLDuration;
                }

                if (pendingStatementID != null)
                {
                    currentStatement.SQLID = pendingStatementID;
                    pendingStatementID = null;
                } else
                {
                    currentStatement.SQLID = "";
                }

                Statements.Add(currentStatement);

                cursorMap[header.Cursor] = currentStatement;

                return;
            }

            /* Not exactly sure how COBOL handles durations. the trace file i have has basically all 0's */
            var execMatch = execStatement.Match(line);
            if (execMatch.Success || compExecMatch.Success)
            {
                if (execMatch.Success)
                {
                    m = execMatch;
                } else
                {
                    m = compExecMatch;
                }
                var currentStatement = cursorMap[header.Cursor];
                currentStatement.ExecTime = header.Duration;
                return;
            }

            m = fetchStatement.Match(line);
            if (m.Success)
            {
                if(cursorMap.ContainsKey(header.Cursor) == false)
                {
                    Debugger.Break();
                }
                var currentStatement = cursorMap[header.Cursor];

                switch(header.RCNumber)
                {
                    case RTNCD_OK:
                        currentStatement.FetchCount++;
                        currentStatement.FetchTime += header.Duration;
                        break;
                    case RTNCD_END:
                        currentStatement.FetchTime += header.Duration;
                        break;
                    default:
                        currentStatement.IsError = true;
                        currentStatement.ErrorInfo = new SQLError() { ReturnCode = header.RCNumber, Message = m.Groups[2].Value };
                        break;

                }
                return;
            }

            m = bindStatement.Match(line);
            if (m.Success)
            {
                var currentStatement = cursorMap[header.Cursor];
                SQLBindValue bind = new SQLBindValue();
                if (m.Groups[2].Value != "")
                {
                    bind.Index = int.Parse(m.Groups[2].Value);
                } else
                {
                    bind.Index = int.Parse(m.Groups[3].Value);
                }

                /* Due to primary support for TraceSQL the Type here is an integer */
                /* It is really only used when reconstructing a SQL with binds replaced */
                /* It boils down to Type == 19 means no quotes around value, Type != 19 means quotes around value */
                bind.TypeString = m.Groups[4].Value + $" ({m.Groups[5].Value})";
                switch (m.Groups[4].Value)
                {
                    case "SQLPSPD":
                        bind.Type = 19;
                        break;
                    case "SQLPSLO":
                        bind.Type = 19;
                        break;
                    case "SQLPSH":
                        bind.Type = 19;
                        break;
                    case "SQLPBUF":
                        bind.Type = 0;
                        break;
                    case "SQLPDAT":
                        bind.Type = 0;
                        break;
                    case "SQLPSTR":
                        bind.Type = 0;
                        break;
                }
                if (m.Groups[6].Value != "")
                {
                    /* Use "precision" for length */
                    bind.Length = int.Parse(m.Groups[6].Value);
                } else
                {
                    bind.Length = int.Parse(m.Groups[8].Value);
                }
                bind.Value = m.Groups[9].Value;
                currentStatement.AddBindValue(bind);
                return;
            }

        }

        public void ProcessorComplete(TraceData data)
        {
            if (Statements.Count == 0)
            {
                return;
            }

            /* Group them all by Where */
            var sqlByWhereList = data.SQLByWhere;

            var byWheres = Statements.Where(p => p.Type != SQLType.INSERT).GroupBy(p => p.WhereClause).Select(g => new SQLByWhere { NumberOfCalls = g.Count(), TotalTime = g.Sum(i => i.Duration), WhereClause = g.Key, HasError = g.Count(p => p.IsError) > 0 ? true : false });
            foreach (var byW in byWheres)
            {
                sqlByWhereList.Add(byW);
            }

            var sqlByFromList = data.SQLByFrom;
            var byFroms = Statements.Where(p => p.Type == SQLType.SELECT || p.Type == SQLType.DELETE).GroupBy(p => p.FromClause).Select(g => new SQLByFrom { NumberOfCalls = g.Count(), TotalTime = g.Sum(i => i.Duration), FromClause = g.Key, HasError = g.Count(p => p.IsError) > 0 ? true : false });
            foreach (var byF in byFroms)
            {
                sqlByFromList.Add(byF);
            }
            var stats = data.Statistics;

            stats.Add(new StatisticItem() { Category = "SQL Statements", Label = "Total Count", Value = Statements.Count.ToString() });
            SQLStatement longest = Statements.OrderBy(s => s.Duration).Reverse().First();
            stats.Add(new StatisticItem() { Category = "SQL Statements", Label = "Longest Execution", Value = longest.Duration.ToString(), Tag = longest });
            SQLStatement mostFetches = Statements.OrderBy(s => s.FetchCount).Reverse().First();
            stats.Add(new StatisticItem() { Category = "SQL Statements", Label = "Most Fetches", Value = mostFetches.FetchCount.ToString(), Tag = mostFetches });
            stats.Add(new StatisticItem() { Category = "SQL Statements", Label = "Total SQL Time", Value = Statements.Sum(s => s.Duration).ToString() });

            stats.Add(new StatisticItem() { Category = "SQL Statements", Label = "Total SELECT Time", Value = Statements.Where(s => s.Type == SQLType.SELECT).Sum(s => s.Duration).ToString() });
            stats.Add(new StatisticItem() { Category = "SQL Statements", Label = "Total UPDATE Time", Value = Statements.Where(s => s.Type == SQLType.UPDATE).Sum(s => s.Duration).ToString() });
            stats.Add(new StatisticItem() { Category = "SQL Statements", Label = "Total INSERT Time", Value = Statements.Where(s => s.Type == SQLType.INSERT).Sum(s => s.Duration).ToString() });
            stats.Add(new StatisticItem() { Category = "SQL Statements", Label = "Total DELETE Time", Value = Statements.Where(s => s.Type == SQLType.DELETE).Sum(s => s.Duration).ToString() });
        }

        public void ProcessorInit(TraceData data)
        {
            Statements = data.SQLStatements;
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
        }
    }
}

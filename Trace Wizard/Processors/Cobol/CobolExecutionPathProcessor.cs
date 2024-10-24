using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using TraceWizard.Data;

namespace TraceWizard.Processors
{
    internal class CobolExecutionPathProcessor : ITraceProcessor
    {
        TraceData traceData;

        Regex connectLine = new Regex(@"\sConnect=");
        Regex disconnectLine = new Regex(@"\sDisconnect$");
        Regex rollbackLine = new Regex(@"\sRollback$");
        Regex commitLine = new Regex(@"\sCommit$");

        Regex newStatement = new Regex("(COM|CEX) Stmt=(.*)");

        ExecutionCall currentCall = null;

        private static bool IsValid(string line)
        {
            if (line == null)
                return false;

            return line.Contains(" Connect=") ||
               line.EndsWith(" Disconnect") ||
               line.EndsWith(" Rollback") ||
               line.EndsWith(" Commit") ||
               line.Contains("COM Stmt=") ||
               line.Contains("CEX Stmt=");
        }

        public void ProcessLine(string line, long lineNumber)
        {
            if (CobolExecutionPathProcessor.IsValid(line) == false) return;
            var header = CobolLineHeader.FromLogLine(line);
            var m = connectLine.Match(line);
            if (m.Success) {
                ExecutionCall executionCall = new ExecutionCall();
                executionCall.Function = $"Start Cursor #{header.Cursor}";
                executionCall.Type = ExecutionCallType.CALL;
                executionCall.StartLine = lineNumber;
                executionCall.StopLine = lineNumber;

                if (currentCall != null)
                {
                    currentCall.Children.Add(executionCall);
                    executionCall.Parent = currentCall;
                    traceData.AllExecutionCalls.Add(executionCall);   
                    currentCall = executionCall;
                } else
                {
                    currentCall = executionCall;
                    executionCall.Context = "Cobol Trace";
                    traceData.ExecutionPath.Add(executionCall);
                    traceData.AllExecutionCalls.Add(executionCall);
                }
                return;
            }

            m = disconnectLine.Match(line);
            if (m.Success)
            {   
                ExecutionCall executionCall = new ExecutionCall();
                executionCall.Type = ExecutionCallType.CALL;
                executionCall.Function = $"Disconnect";
                executionCall.StartLine = lineNumber;
                executionCall.StopLine = lineNumber; 

                if (currentCall != null)
                {
                    currentCall.Children.Add(executionCall);
                    executionCall.Parent = currentCall;
                    traceData.AllExecutionCalls.Add(executionCall);
                }
                else
                {
                    executionCall.Context = "Cobol Trace";
                    traceData.ExecutionPath.Add(executionCall);
                    traceData.AllExecutionCalls.Add(executionCall);
                }

                if (currentCall != null)
                {
                    currentCall = currentCall.Parent;
                }

                return;
            }


            m = rollbackLine.Match(line);
            if (m.Success)
            {
                ExecutionCall executionCall = new ExecutionCall();
                executionCall.Type = ExecutionCallType.CALL;
                executionCall.Function = $"Rollback"; ;
                executionCall.StartLine = lineNumber;
                executionCall.StopLine = lineNumber;

                if (currentCall != null)
                {
                    currentCall.Children.Add(executionCall);
                    executionCall.Parent = currentCall;
                    traceData.AllExecutionCalls.Add(executionCall);
                }
                else
                {
                    executionCall.Context = "Cobol Trace";
                    traceData.ExecutionPath.Add(executionCall);
                    traceData.AllExecutionCalls.Add(executionCall);
                }
                return;
            }

            m = commitLine.Match(line);
            if (m.Success)
            {
                ExecutionCall executionCall = new ExecutionCall();
                executionCall.Type = ExecutionCallType.CALL;
                executionCall.Function = $"Commit"; ;
                executionCall.StartLine = lineNumber;
                executionCall.StopLine = lineNumber;

                if (currentCall != null)
                {
                    currentCall.Children.Add(executionCall);
                    executionCall.Parent = currentCall;
                    traceData.AllExecutionCalls.Add(executionCall);
                }
                else
                {
                    executionCall.Context = "Cobol Trace";
                    traceData.ExecutionPath.Add(executionCall);
                    traceData.AllExecutionCalls.Add(executionCall);
                }
                return;
            }
            m = newStatement.Match(line);
            if (m.Success)
            {
                ExecutionCall executionCall = new ExecutionCall();
                executionCall.Function = m.Groups[8].Value;
                executionCall.Type = ExecutionCallType.SQL;
                executionCall.StartLine = lineNumber;
                executionCall.StopLine = lineNumber;
                executionCall.Duration = header.SQLDuration;
                executionCall.Parent = currentCall;

                /* This is safe to do because the SQL processor is always run first */
                executionCall.SQLStatement = traceData.SQLStatements[traceData.SQLStatements.Count - 1];

                traceData.AllExecutionCalls.Add(executionCall);
                if (currentCall != null)
                {
                    currentCall.Children.Add(executionCall);
                }
                else
                {
                    executionCall.Context = "Cobol Trace";
                    traceData.ExecutionPath.Add(executionCall);
                }
            }

        }

        public void ProcessorComplete(TraceData data)
        {
            
        }

        public void ProcessorInit(TraceData data)
        {
            traceData = data;
        }
    }
}

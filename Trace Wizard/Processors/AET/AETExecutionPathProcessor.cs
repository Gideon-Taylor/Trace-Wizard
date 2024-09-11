using System;
using System.Linq;
using System.Text.RegularExpressions;
using TraceWizard.Data;

namespace TraceWizard.Processors
{
    internal class AETExecutionPathProcessor : ITraceProcessor
    {
        TraceData _data = null;

        Regex appEngineStart = new Regex(@"-- \d{4}-\d{2}-\d{2} (\d{2}:\d{2}:\d{2}.\d\d\d) Tracing Application Engine program ([^ ]+) (.*)");
        Regex stepMarker = new Regex(@"-- \d{4}-\d{2}-\d{2} (\d{2}:\d{2}:\d{2}.\d\d\d) (\.+)\(([^.]+)\.([^.]+)\.([^.)]+)\) \((.*?)\)");
        ExecutionContext ctx = null;
        int lastIndent = 0;

        public void ProcessLine(string line, long lineNumber)
        {
            var match = appEngineStart.Match(line);
            if (match.Success)
            {
                ctx.ContextID = "App Engine " + match.Groups[2].Value + "(" + match.Groups[3] + ")";
                return;
            }

            match = stepMarker.Match(line);
            if (match.Success)
            {
                ctx.ProcessAECall(match, lineNumber);
            }
        }

        public void ProcessorComplete(TraceData data)
        {
            _data.ExecutionPath.AddRange(ctx.executionCalls);
            _data.AllExecutionCalls.AddRange(ctx.allCalls);

            foreach (ExecutionCall call in ctx.allCalls.Where(c => c.Type.HasFlag(ExecutionCallType.SQL)))
            {
                var lineNumber = call.StartLine;
                try
                {
                    var sqlStatement = data.SQLStatements.Where(s => s.LineNumber == lineNumber + 1).First();

                    call.SQLStatement = sqlStatement;
                    sqlStatement.ParentCall = call.Parent;
                    call.Duration = sqlStatement.Duration;
                }catch (Exception ex)
                {
                    /* this happens durring loops where there was no SQL statement. We need to find a SQL statement prior and clone it and update binds when we capture those */
                    var dummyStatement = new SQLStatement("SELECT 'X' FROM DUAL");
                    call.SQLStatement = dummyStatement;
                    dummyStatement.ParentCall = call.Parent;
                    call.Duration = dummyStatement.Duration;
                }
            }
        }

        public void ProcessorInit(TraceData data)
        {
            ctx = new ExecutionContext("App Engine Trace");
            _data = data;
        }
    }
}
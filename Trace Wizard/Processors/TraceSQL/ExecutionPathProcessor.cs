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
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using TraceWizard.Data;

namespace TraceWizard.Processors
{
    internal class ExecutionContext
    {
        internal string ContextID;

        internal List<ExecutionCall> allCalls = new List<ExecutionCall>();
        internal List<ExecutionCall> executionCalls = new List<ExecutionCall>();
        internal List<ExecutionCall> sqlCalls = new List<ExecutionCall>();

        Stack<ExecutionCall> callChain = new Stack<ExecutionCall>();
        ExecutionCall lastPopped;
        internal int _maxCallDepth;
        internal long _ppcCodeCallCount;
        internal long _ppcExceptionCount;
        internal bool ParsingReturnStack;
        internal bool ReturnStackIsClass;
        internal long _aeStepCount;
        internal string ReturnStackClassName = null;
        internal string ReturnStackClassMethod = null;
        internal ExecutionCall ReturnStackCall = null;

        public bool ParsingReturnValue { get; internal set; }

        internal ExecutionContext(string contextID)
        {
            ContextID = contextID;
        }

        internal void ProcessCallMarker(Match match, long lineNumber)
        {
            _ppcCodeCallCount++;
            var call = new ExecutionCall() { Context = ContextID, Type = ExecutionCallType.CALL, StartLine = lineNumber, Function = match.Groups[3].Value + ": " + match.Groups[4].Value, indentCount = match.Groups[2].Value.Length };
            allCalls.Add(call);
            call.Duration = Double.Parse(match.Groups[1].Value);
            if (callChain.Count > 0 && callChain.Peek().indentCount == call.indentCount)
            {
                var popped = callChain.Pop();
                if (popped.StopLine == 0)
                {
                    popped.StopLine = lineNumber;
                }
                lastPopped = popped;
            }

            if (callChain.Count > 0 && callChain.Peek().Type == ExecutionCallType.CALL)
            {
                while (callChain.Count > 0 && callChain.Peek().indentCount >= call.indentCount)
                {
                    var popped = callChain.Pop();
                    if (popped.StopLine == 0)
                    {
                        popped.StopLine = lineNumber;
                    }
                    lastPopped = popped;
                }
            }

            if (callChain.Count > 0)
            {
                callChain.Peek().Children.Add(call);
                call.Parent = callChain.Peek();
            }
            if (callChain.Count == 0)
            {
                executionCalls.Add(call);
            }
            callChain.Push(call);
            ResetReturnStack();
            return;
        }

        internal void ProcessStartOrResume(Match match, long lineNumber)
        {
            _ppcCodeCallCount++;
            // we have the start of a function
            var call = new ExecutionCall() { Context = ContextID, Type = ExecutionCallType.NORMAL, StartLine = lineNumber, Nest = match.Groups[1].Value, Function = match.Groups[2].Value };
            allCalls.Add(call);
            if (callChain.Count == 0)
            {
                executionCalls.Add(call);
            }
            callChain.Push(call);

            if (callChain.Count > _maxCallDepth)
            {
                _maxCallDepth = callChain.Count;
            }
            ResetReturnStack();
        }

        internal void ProcessEndOrReend(Match match, long lineNumber)
        {
            // we've reached the end of a call, we need to find it in the list and mark the ending line
            var nest = match.Groups[1].Value;
            var func = match.Groups[2].Value;
            var dur = match.Groups.Count == 4 ? match.Groups[3].Value : "0";

            bool callFound = false;
            ExecutionCall call = null;
            while (callFound == false && callChain.Count > 0)
            {
                call = callChain.Pop();
                lastPopped = call;
                if (call.Nest == nest && call.Function == func)
                {
                    callFound = true;
                    call.StopLine = lineNumber;
                    call.Duration = Double.Parse(dur);
                }

            }

            if (callChain.Count > 0)
            {
                /* If there are calls on the call chain                           *
                 * then we should associate the current call with its parent      *
                 */
                var parent = callChain.Peek();
                parent.Children.Add(call);
                call.Parent = parent;
            }
            ResetReturnStack();
        }

        internal void ProcessStartExtMarker(Match match, long lineNumber)
        {
            _ppcCodeCallCount++;
            // we have the start of a function
            var call = new ExecutionCall() { Context = ContextID, Type = ExecutionCallType.EXTERNAL, StartLine = lineNumber, Nest = match.Groups[1].Value, Function = match.Groups[2].Value };
            allCalls.Add(call);
            if (callChain.Count == 0)
            {
                executionCalls.Add(call);
            }

            /* test */
            if (callChain.Count > 0 && callChain.Peek().Type == ExecutionCallType.CALL)
            {
                var parent = callChain.Pop();
                lastPopped = parent;
                if (parent.StopLine == 0)
                {
                    parent.StopLine = lineNumber;
                }
                parent.Children.Add(call);
                call.Parent = parent;
            }

            callChain.Push(call);
            if (callChain.Count > _maxCallDepth)
            {
                _maxCallDepth = callChain.Count;
            }
            ResetReturnStack();
        }

        internal void ProcessEndExtMarker(Match match, long lineNumber)
        {
            // we've reached the end of a call, we need to find it in the list and mark the ending line
            var nest = match.Groups[1].Value;
            var func = match.Groups[2].Value;
            var dur = match.Groups[3].Value;

            if (callChain.Count > 0 && callChain.Peek().Type == ExecutionCallType.CALL)
            {
                while (callChain.Count > 0 && (callChain.Peek().Type == ExecutionCallType.CALL || callChain.Peek().Nest != nest))
                {
                    var popped = callChain.Pop();
                    if (popped.StopLine == 0)
                    {
                        popped.StopLine = lineNumber;
                    }
                    lastPopped = popped;
                }
            }
            if (callChain.Count == 0)
            {
                Debugger.Break();
            }
            var call = callChain.Pop();
            lastPopped = call;
            call.StopLine = lineNumber;
            call.Duration = Double.Parse(dur);
            ResetReturnStack();
        }

        internal void ProcessSQLStatement(Match match, long lineNumber)
        {
            var sqlCall = new ExecutionCall() { Context = ContextID, Type = ExecutionCallType.SQL, StartLine = lineNumber, StopLine = lineNumber, Function = "SQL" };
            allCalls.Add(sqlCall);
            if (callChain.Count > 0)
            {
                sqlCall.Parent = callChain.Peek();
                callChain.Peek().Children.Add(sqlCall);
            }
            else
            {
                executionCalls.Add(sqlCall);
            }
            sqlCalls.Add(sqlCall);
            ResetReturnStack();
        }

        internal void ProcessPPCException(Match match, long lineNumber)
        {
            _ppcExceptionCount++;
            var exception = new PPCException() { Message = match.Groups[1].Value };
            if (callChain.Count > 0)
            {
                var call = callChain.Peek();
                call.PPCException = exception;
                call.IsError = true;
                var parent = call.Parent;
                while (parent != null)
                {
                    parent.HasError = true;
                    parent = parent.Parent;
                }
            }
            ResetReturnStack();
        }

        internal void ProcessReturnStackLine(Match match, long lineNumber)
        {
            if (ReturnStackIsClass == false)
            {
                if (match.Groups[1].Value.StartsWith("UUU") && match.Groups[2].Value == ":")
                {
                    ReturnStackIsClass = true;
                    ReturnStackClassName = match.Groups[3].Value;
                }
                else
                {
                    if (callChain.Count > 0 && callChain.Peek().Parent != null && callChain.Peek().Parent.Function.StartsWith("constructor:"))
                    {
                        /* this is a constructor parameter? */
                        ReturnStackIsClass = true;
                        ReturnStackClassMethod = "";
                        ReturnStackCall = callChain.Peek().Parent;
                        ReturnStackCall.Parameters = new List<string>();
                        ProcessReturnStackLine(match, lineNumber);
                    }
                    else
                    {
                        ReturnStackIsClass = false;
                        ReturnStackClassName = null;
                        ReturnStackClassMethod = null;
                        ReturnStackCall = null;
                    }
                }
            }
            else
            {
                /* already identified this return stack as a class */
                if (ReturnStackClassMethod == null)
                {
                    ReturnStackClassMethod = match.Groups[3].Value;
                }
                else
                {
                    if (ReturnStackCall == null)
                    {

                        /* find the most recent call that matches that doesn't have parameters... */
                        ReturnStackCall = allCalls.Where(c => c.Parameters == null && c.Function.Contains($":{ReturnStackClassName}.{ReturnStackClassMethod}")).LastOrDefault();
                       if (ReturnStackCall == null)
                        {
                            /* Check for a private method */
                            ReturnStackCall = allCalls.Where(c => c.Parameters == null && c.Function.Contains($"private: {ReturnStackClassMethod}")).LastOrDefault();
                        }

                        /* add this parameter */
                        if (ReturnStackCall != null)
                        {
                            ReturnStackCall.Parameters = new List<string>();
                            if (match.Groups.Count > 2)
                            {
                                /* this is a Something = Something parameter */
                                ReturnStackCall.Parameters.Add($"{match.Groups[1].Value} = {match.Groups[3].Value}");
                            } else
                            {
                                /* this parameter is a builtin like array or record etc */
                                ReturnStackCall.Parameters.Add($"{match.Groups[1].Value}");
                            }
                            
                        }
                    }
                    else
                    {
                        /* we've found the call, this must be a parameter... add it */
                        if (match.Groups.Count > 1)
                        {
                            /* this is a Something = Something parameter */
                            ReturnStackCall.Parameters.Add($"{match.Groups[1].Value} = {match.Groups[3].Value}");
                        }
                        else
                        {
                            /* this parameter is a builtin like array or record etc */
                            ReturnStackCall.Parameters.Add($"{match.Groups[1].Value}");
                        }
                    }
                }
            }


        }

        internal void ResetReturnStack()
        {
            ReturnStackCall = null;
            ReturnStackClassMethod = null;
            ReturnStackClassName = null;
            ReturnStackIsClass = false;
            ParsingReturnStack = false;
            ParsingReturnValue = false;
        }

        internal void ProcessReturnValue(Match match, long lineNumber)
        {
            if (ReturnStackCall != null)
            {
                ReturnStackCall.ReturnValue = match.Groups[1].Value;
                ParsingReturnValue = false;
                ResetReturnStack();
            }
        }

        internal void ProcessAECall(Match match, long lineNumber)
        {
            var timeStamp = match.Groups[1].Value;
            var indent = match.Groups[2].Value.Length;
            var program = match.Groups[3].Value;
            var section = match.Groups[4].Value;
            var step = match.Groups[5].Value;
            var action = match.Groups[6].Value;

            ExecutionCallType callType;

            switch(action)
            {
                case "Do When":
                    callType = ExecutionCallType.SQL | ExecutionCallType.AE_DOWHEN;
                    break;
                case "Do While":
                    callType = ExecutionCallType.SQL | ExecutionCallType.AE_DOWHILE;
                    break;
                case "Do Select":
                    callType = ExecutionCallType.SQL | ExecutionCallType.AE_DOSELECT;
                    break;
                case "PeopleCode":
                    callType = ExecutionCallType.AE_PPC;
                    break;
                case "SQL":
                    callType = ExecutionCallType.SQL | ExecutionCallType.AE_SQL;
                    break;
                case "Call Section":
                    callType = ExecutionCallType.AE_CALL;
                    break;
                case "Log Message":
                    callType = ExecutionCallType.AE_LOGMSG;
                    break;
                case "Do Until":
                    callType = ExecutionCallType.SQL | ExecutionCallType.AE_DOUNTIL;
                    break;
                default:
                    callType = ExecutionCallType.NORMAL;
                    break;
            }
            callType |= ExecutionCallType.AE;

            var call = new ExecutionCall() {
                Context = ContextID,
                Type = callType,
                StartLine = lineNumber,
                AE_Program = program,
                AE_Section = section,
                AE_Step = step,
                AE_Action = action,
                Function = program + "." + section + "." + step + " (" + action + ")",
                indentCount = indent
            };
            allCalls.Add(call);
            call.Duration = 0;

            _aeStepCount++;
            if (callType == ExecutionCallType.AE_PPC)
            {
                _ppcCodeCallCount++;
            }

            if (callChain.Count > 0 && callChain.Peek().indentCount == call.indentCount)
            {
                var popped = callChain.Pop();
                if (popped.StopLine == 0)
                {
                    popped.StopLine = lineNumber;
                }
                lastPopped = popped;
            }

            if (callChain.Count > 0 && (callChain.Peek().Type == ExecutionCallType.AE_CALL || call.indentCount < callChain.Peek().indentCount))
            {
                while (callChain.Count > 0 && callChain.Peek().indentCount >= call.indentCount)
                {
                    var popped = callChain.Pop();
                    if (popped.StopLine == 0)
                    {
                        popped.StopLine = lineNumber;
                    }
                    lastPopped = popped;
                }
            }

            if (callChain.Count > 0)
            {
                callChain.Peek().Children.Add(call);
                call.Parent = callChain.Peek();
            }
            if (callChain.Count == 0)
            {
                executionCalls.Add(call);
            }
            callChain.Push(call);

            //throw new NotImplementedException();
        }
    }


    public class ExecutionPathProcessor : ITraceProcessor
    {
        //Regex contextMarker = new Regex("PSAPPSRV\\.\\d+ \\(\\d+\\)");
        Regex contextMarker = new Regex(@"^PSAPPSRV\.\d+.+?\(\d+\)");
        Regex startMarker = new Regex(">>> start\\s+Nest=(\\d+)\\s+(.*)");
        Regex startExtMarker = new Regex(">>> start-ext\\sNest=(\\d+)\\s(.*)");
        Regex endMarker = new Regex("<<< end\\s+Nest=(\\d+)\\s+(.*?)\\s+Dur=(\\d+\\.\\d+)");
        Regex endExtMarker = new Regex("<<< end-ext\\s+Nest=(\\d+)\\s+(.*?)\\s+Dur=(\\d+\\.\\d+)");
        Regex callMarker = new Regex("(\\d+\\.\\d+)(\\s+)call\\s+(.*?)\\s+(.*)");
        Regex sqlStatement = new Regex("RC=(\\d+) Dur=(\\d+\\.\\d+) COM Stmt=(.*)");
        Regex ppcExceptionStatement = new Regex("ErrorReturn->(.*)");
        Regex parameterStatement = new Regex("\\s+[^=]+\\=\\[^=]*");

        Regex resumeMarker = new Regex(">>> resume\\s+Nest=(\\d+)\\s+(.*)");
        Regex reendMarker = new Regex(">>> reend\\s+Nest=(\\d+)\\s+(.*)");

        Regex returnStack = new Regex("return stack:");
        Regex returnStackLinePrimitive = new Regex("\\d+\\.\\d+\\s{42}(.*?)([=:])(.*)");
        Regex returnStackLineBuiltin = new Regex("\\d+\\.\\d+\\s{42}(.*)");

        Regex returnValue = new Regex("return value:");
        Regex returnValueResult = new Regex("\\d+\\.\\d+\\s{42}(.*)");

        Dictionary<string, ExecutionContext> contextMap = new Dictionary<string, ExecutionContext>();

        bool isAETrace = false;

        public ExecutionPathProcessor(bool isAETrace = false)
        {
            this.isAETrace = isAETrace;
        }

        public void ProcessorInit(TraceData data)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");

            if (this.isAETrace)
            {
                /* Create an App Engine "context" here */
            }

        }

        public void ProcessorComplete(TraceData td)
        {

            foreach (ExecutionContext ctx in contextMap.Values)
            {
                foreach (ExecutionCall call in ctx.sqlCalls)
                {
                    var lineNumber = call.StartLine;

                    var sqlStatement = td.SQLStatements.Where(s => s.LineNumber == lineNumber).First();

                    call.SQLStatement = sqlStatement;
                    sqlStatement.ParentCall = call.Parent;
                    call.Duration = sqlStatement.Duration;
                    if (sqlStatement.IsError)
                    {
                        call.HasError = true;
                        var parent = call.Parent;
                        while (parent != null)
                        {
                            parent.HasError = true;
                            parent = parent.Parent;
                        }
                    }
                }
            }



            /* process stack traces from TraceDatta */
            foreach (StackTraceEntry traceEntry in td.StackTraces)
            {
                var x = FindCallForLineNumber(traceEntry.LineNumber);
                if (x == null)
                {
                    // sometimes they get printed right after the exit, try 1 line number above
                    x = FindCallForLineNumber(traceEntry.LineNumber-1);
                }
                if (x != null)
                {
                    x.IsError = true;
                    x.StackTrace = traceEntry;
                    traceEntry.Parent = x;
                    var parent = x.Parent;
                    while (parent != null)
                    {
                        parent.HasError = true;
                        parent = parent.Parent;
                    }
                }
            }
            if (contextMap.Values.Count > 0)
            {
                td.MaxCallDepth = contextMap.Values.Max(m => m._maxCallDepth);
            }

            td.Statistics.Add(new StatisticItem() { Category = "Execution Path", Label = "Maximum Call Depth", Value = td.MaxCallDepth.ToString() });
            td.Statistics.Add(new StatisticItem() { Category = "Execution Path", Label = "Total Calls", Value = contextMap.Values.Sum(c=>c._ppcCodeCallCount).ToString() });
            td.Statistics.Add(new StatisticItem() { Category = "Execution Path", Label = "PeopleCode Exceptions", Value = contextMap.Values.Sum(c => c._ppcExceptionCount).ToString() });
            

            /* copy all calls to the TraceData structure */
            /*executionCalls = data.ExecutionPath;
            allCalls = data.AllExecutionCalls;*/
            foreach(ExecutionContext ctx in contextMap.Values)
            {
                td.ExecutionPath.AddRange(ctx.executionCalls);
                td.AllExecutionCalls.AddRange(ctx.allCalls);
            }

            td.Statistics.Add(new StatisticItem() { Category = "Execution Path", Label = "Total PeopleCode Time", Value = td.AllExecutionCalls.Where(c => c.Parent == null).Sum(c => c.Duration).ToString() });
        }

        ExecutionCall FindCallForLineNumber(long lineNumber)
        {
            ExecutionCall call = null;
            foreach (ExecutionContext ctx in contextMap.Values)
            {
                call = ctx.allCalls.Where(c => c.StartLine <= lineNumber && c.StopLine >= lineNumber).LastOrDefault();
                if (call != null)
                {
                    break;
                }
            }

            return call;
        }

        public void ProcessLine(string line, long lineNumber)
        {
            if (lineNumber == 1)
            {
                if (line.Contains("AE SQL/PeopleCode Trace"))
                {
                    this.isAETrace = true;
                    return;
                }
            }

            if (!this.isAETrace && contextMarker.IsMatch(line) == false)
            {
                return;
            }

            var currentContextString = "";

            if (!this.isAETrace)
            {
                currentContextString = contextMarker.Match(line).Groups[0].Value;
            } else
            {
                currentContextString = "App Engine Execution";
            }

            if (contextMap.ContainsKey(currentContextString) == false)
            {
                contextMap.Add(currentContextString, new ExecutionContext(currentContextString));
            }

            var context = contextMap[currentContextString];

            var match = callMarker.Match(line);
            if (match.Success)
            {
                context.ProcessCallMarker(match, lineNumber);
                return;
            }

            match = startMarker.Match(line);
            if (match.Success == false)
            {
                match = resumeMarker.Match(line);
            }
            /* Start marker, or Resume Marker */
            if (match.Success)
            {
                context.ProcessStartOrResume(match, lineNumber);
                return;
            }

            match = endMarker.Match(line);
            if (match.Success == false)
            {
                match = reendMarker.Match(line);
            }
            /* end marker or reend marker */
            if (match.Success)
            {
                context.ProcessEndOrReend(match, lineNumber);
                return;
            }

            match = startExtMarker.Match(line);
            if (match.Success)
            {
                context.ProcessStartExtMarker(match, lineNumber);
                return;
            }

            match = endExtMarker.Match(line);
            if (match.Success)
            {
                context.ProcessEndExtMarker(match, lineNumber);
                return;
            }

            match = sqlStatement.Match(line);
            if (match.Success)
            {
                context.ProcessSQLStatement(match, lineNumber);
                return;
            }
            match = ppcExceptionStatement.Match(line);
            if (match.Success)
            {
                context.ProcessPPCException(match, lineNumber);
                return;

            }

            match = returnStack.Match(line);
            if (match.Success)
            {
                context.ResetReturnStack();
                context.ParsingReturnStack = true;
                return;
            }
            if (context.ParsingReturnStack)
            {
                match = returnStackLinePrimitive.Match(line);
                if (match.Success)
                {
                    context.ProcessReturnStackLine(match, lineNumber);
                } else
                {
                    match = returnStackLineBuiltin.Match(line);
                    if (match.Success)
                    {
                        context.ProcessReturnStackLine(match, lineNumber);
                    } else
                    {
                        match = returnValue.Match(line);
                        if (match.Success)
                        {
                            context.ParsingReturnStack = false;
                            context.ParsingReturnValue = true;
                            return;
                        } else
                        {
                            context.ResetReturnStack();
                        }
                    }
                    
                }
            }

            if (context.ParsingReturnValue)
            {
                match = returnValueResult.Match(line);
                if (match.Success)
                {
                    context.ProcessReturnValue(match, lineNumber);
                }
            }

            return;
        }
    }
    
}

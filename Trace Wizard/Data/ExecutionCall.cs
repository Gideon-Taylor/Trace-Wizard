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
using System.Linq;

namespace TraceWizard.Data
{
    [Serializable]
    public class ExecutionCall
    {
        public static uint NextId;

        /* Only used for SQL type calls, this is hooked up in post-process */
        public SQLStatement SQLStatement;

        /* Only used if this call resulted in a StackTrace */
        public StackTraceEntry StackTrace;

        /* Used to hold PPC exceptions */
        public PPCException PPCException;

        public uint InternalID = NextId++;

        public bool HasError;
        public bool IsError;
        public int indentCount;
        public string Context = "";
        public string Nest = "";
        public string Function = "";

        public string AE_Program = "";
        public string AE_Section = "";
        public string AE_Step = "";
        public string AE_Action = "";

        protected double _duration;

        public List<string> Parameters;
        public string ReturnValue;

        public double InternalDuration
        {
            get
            {
                return _duration;
            }
        }

        public double Duration
        {
            get
            {
                if (Type == ExecutionCallType.CALL)
                {
                    double total = 0;
                    foreach (var c in Children)
                    {
                        total += c.Duration;
                    }
                    return total;
                } else
                {
                    return _duration;
                }
                
            }
            set
            {
                _duration = value;
            }
        }

        public long StartLine;
        private long _stopLine;
        public long InternalStopLine
        {
            get
            {
                return _stopLine;
            }
        }
        public long StopLine { get
            {
                if (Type != ExecutionCallType.CALL)
                {
                    return _stopLine;
                } else
                {
                    if (Children.Count > 0)
                    {
                        return Children.Last().StopLine;
                    }
                }
                return StartLine + 1;
            }
            set
            {
                if (Type != ExecutionCallType.CALL)
                {
                    _stopLine = value;
                }
            }
        }
        public ExecutionCallType Type;

        public ExecutionCall Parent;
        public List<ExecutionCall> Children = new List<ExecutionCall>();

        public List<Tuple<long, string>> GetStackTrace()
        {
            List<Tuple<long, string>> trace = new List<Tuple<long, string>>();
            trace.Add(Tuple.Create<long, string>(StartLine, Function));
            var parent = Parent;
            while (parent != null)
            {
                trace.Add(Tuple.Create<long, string>(parent.StartLine, parent.Function));
                parent = parent.Parent;
            }

            trace.Reverse();
            return trace;
        }

        /* Pieces used when Diffing */
        public DiffStatus DiffStatus = DiffStatus.SAME;


    }

    public enum DiffStatus
    {
        SAME,INSERT,DELETE,MODIFIED
    }

    [Flags]
    public enum ExecutionCallType
    {
        NORMAL = 1, 
        EXTERNAL = 2, 
        CALL = 4, 
        SQL = 8, 
        AE = 16, 
        AE_DOWHEN = 32, 
        AE_DOWHILE = 64, 
        AE_DOSELECT = 128, 
        AE_PPC = 256, 
        AE_SQL = 512, 
        AE_CALL = 1024, 
        AE_LOGMSG = 2048, 
        AE_DOUNTIL = 4096,
        COBOL = 8192,
    }
}

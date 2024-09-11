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

using System.Collections.Generic;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading;
using TraceWizard.Data;

namespace TraceWizard.Processors
{
    public class StackTraceProcessor : ITraceProcessor
    {
        private bool _processingException = false;
        private StackTraceEntry currentStackTrace;
        private int _stackTraceBuffer = 5;
        public List<StackTraceEntry> StackTraces;

        public void ProcessorInit(TraceData data)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            StackTraces = data.StackTraces;
        }

        public void ProcessorComplete(TraceData td)
        {
            td.Statistics.Add(new StatisticItem() { Category = "Execution Path", Label = "Stack Trace Count", Value = StackTraces.Count.ToString() });
        }

        public void ProcessLine(string line, long lineNumber)
        {
            
            if (_processingException)
            {
                Regex stackTraceContinue = new Regex("Called from:(.*)");

                var match = stackTraceContinue.Match(line);

                if (match.Success)
                {
                    currentStackTrace.StackTrace.Add(match.Groups[1].Value);
                    return;
                } else
                {
                    if (_stackTraceBuffer > 0)
                    {
                        _stackTraceBuffer--;
                        ProcessNewException(line,lineNumber);
                        return;
                    } else
                    {
                        _processingException = false;
                    }
                    
                }

            } else
            {
                ProcessNewException(line, lineNumber);
                return;
            }

        }
        private void ProcessNewException(string line, long lineNumber)
        {
            Regex exceptionStart = new Regex("Caught Exception:\\s+(.*?)\\s+\\(\\d+,\\d+\\)\\s+(.*)");

            var match = exceptionStart.Match(line);
            if (match.Success)
            {
                _processingException = true;
                currentStackTrace = new StackTraceEntry();

                currentStackTrace.LineNumber = lineNumber;
                currentStackTrace.Message = match.Groups[1].Value;
                currentStackTrace.Offender = match.Groups[2].Value;

                StackTraces.Add(currentStackTrace);
            }

        }
    }
}

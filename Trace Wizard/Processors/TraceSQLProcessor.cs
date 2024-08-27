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
using System.ComponentModel;
using System.IO;
using System.Linq;
using TraceWizard.Data;

namespace TraceWizard.Processors
{
    public class TraceSQLProcessor : BackgroundWorker
    {
        string _file;
        private TraceData Data = new TraceData();
        public bool IsAETrace = false;
        public TraceSQLProcessor(string filename)
        {
            _file = filename;
            this.DoWork += TraceProcessor_DoWork;
        }

        private void TraceProcessor_DoWork(object sender, DoWorkEventArgs e)
        {
            var lineCount = File.ReadLines(_file).Count();

            List<ITraceProcessor> Processors = new List<ITraceProcessor>();
            Processors.Add(new SQLProcessor());
            Processors.Add(new StackTraceProcessor());
            Processors.Add(new ComponentTraceVariableProcessor());
            /* NOTE: This one is purposefully last because it relies on SQL and StackTrace data in post process */
            Processors.Add(new ExecutionPathProcessor(this.IsAETrace));
            foreach (ITraceProcessor proc in Processors)
            {
                proc.ProcessorInit(Data);
            }
            long reportIncrement = (long)(lineCount * .01);
            long linesUntilReport = (long)(lineCount * .01);
            long lineNumber = 0;
            using (StreamReader sr = new StreamReader(_file))
            {
                while (sr.EndOfStream == false)
                {
                    if (this.WorkerSupportsCancellation && this.CancellationPending)
                    {
                        e.Result = null;
                        e.Cancel = true;
                        return;
                    }
                    string line = sr.ReadLine();
                    lineNumber++;
                    linesUntilReport--;
                    if (this.WorkerReportsProgress && linesUntilReport == 0)
                    {
                        this.ReportProgress((int)(((double)lineNumber / (double)lineCount) * 100));
                        linesUntilReport = reportIncrement;
                    }
                    foreach (ITraceProcessor proc in Processors)
                    {
                        proc.ProcessLine(line, lineNumber);

                    }                    
                }
            }
            foreach (ITraceProcessor proc in Processors)
            {
                proc.ProcessorComplete(Data);
            }
            for (var x = 0; x < Processors.Count; x++ ) {
                Processors[x] = null;
            }
            Processors = null;
            //exec.ResolveSQLStatements(sql);
            System.GC.Collect();
            e.Result = Data;
        }
    }

}

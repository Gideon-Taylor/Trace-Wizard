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
using System.Diagnostics;
using System.IO;
using System.Linq;
using TraceWizard.Data;

namespace TraceWizard.Processors
{
    public enum TraceType
    {
        TRACESQL, AET, COBOL
    }
    public class TraceProcessor : BackgroundWorker
    {
        string _file;
        private TraceData Data = new TraceData();
        private IEnumerable<ITraceProcessor> _processors;
        public FormTab startTab;
        public TraceProcessor(string filename, IEnumerable<ITraceProcessor> processors, FormTab startTab = FormTab.STATS)
        {
            _file = filename;
            _processors = processors;
            this.DoWork += TraceProcessor_DoWork;
            this.startTab = startTab;
        }

        private void TraceProcessor_DoWork(object sender, DoWorkEventArgs e)
        {
            var lineCount = File.ReadLines(_file).Count();


            foreach (ITraceProcessor proc in _processors)
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
                    foreach (ITraceProcessor proc in _processors)
                    {
                        proc.ProcessLine(line, lineNumber);

                    }
                }
            }
            foreach (ITraceProcessor proc in _processors)
            {
                proc.ProcessorComplete(Data);
            }


            _processors = null;
            //exec.ResolveSQLStatements(sql);
            System.GC.Collect();
            e.Result = Data;
        }

        public static IEnumerable<ITraceProcessor> MakeSetFor(TraceType type)
        {
            switch (type)
            {
                case TraceType.AET:
                    return new List<ITraceProcessor> { new AETSQLProcessor(), new AETExecutionPathProcessor() };
                case TraceType.TRACESQL:
                    return new List<ITraceProcessor> { new SQLProcessor(),
                    new StackTraceProcessor(),
                    new ComponentTraceVariableProcessor(),
                    new ExecutionPathProcessor()
                    };
                case TraceType.COBOL:
                    return new List<ITraceProcessor>
                    {
                        new CobolSQLProcessor()/*,new CobolExecutionPathProcessor()*/
                    };

            }
            return null;
        }

        public static BackgroundWorker ForFile(string filename)
        {
            BackgroundWorker processor = null;
            var fileExtension = new FileInfo(filename).Extension.ToLower();
            if (fileExtension.Equals(".aet"))
            {
                processor = new TraceProcessor(filename, TraceProcessor.MakeSetFor(TraceType.AET), FormTab.EXEC_PATH);
            }

            if (fileExtension.Equals(".tracesql"))
            {
                processor = new TraceProcessor(filename, TraceProcessor.MakeSetFor(TraceType.TRACESQL), FormTab.STATS);
            }

            if (fileExtension.Equals(".trc"))
            {
                /* This could be an app engine tracesql, or a cobol trace */
                StreamReader sr = new StreamReader(File.OpenRead(filename));
                var firstLine = sr.ReadLine();
                sr.Close();

                if (firstLine.Contains("AE SQL/PeopleCode Trace"))
                {
                    processor = new TraceProcessor(filename, TraceProcessor.MakeSetFor(TraceType.TRACESQL), FormTab.STATS);
                } else if (firstLine.Contains("PeopleSoft Batch Timings Report"))
                {
                    processor = new TraceProcessor(filename, TraceProcessor.MakeSetFor(TraceType.COBOL), FormTab.SQL);
                }               
            }
            if (processor != null)
            {
                processor.WorkerReportsProgress = true;
                processor.WorkerSupportsCancellation = true;
            }
            return processor;
        }
    }
}
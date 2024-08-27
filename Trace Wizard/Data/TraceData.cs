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

namespace TraceWizard.Data
{
    [Serializable]
    public class TraceData
    {
        public List<SQLStatement> SQLStatements = new List<SQLStatement>();
        public List<SQLByWhere> SQLByWhere = new List<SQLByWhere>();
        public List<SQLByFrom> SQLByFrom = new List<SQLByFrom>();

        public List<ExecutionCall> ExecutionPath = new List<ExecutionCall>();
        public List<ExecutionCall> AllExecutionCalls = new List<ExecutionCall>();
        public List<StackTraceEntry> StackTraces = new List<StackTraceEntry>();

        public List<StatisticItem> Statistics = new List<StatisticItem>();

        public List<VariableBundle> Variables = new List<VariableBundle>();

        public long MaxCallDepth;
    }

    
}

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TraceWizard.Data;

namespace TraceWizard.Data.Serialization
{
    public class TraceSerializer : IDisposable 
    {
        const uint MAGIC = 0x40631374;
        const uint VERSION = 0x01;

        MemoryStream ms = new MemoryStream();

        public void SerializeTraceData(TraceData data, Stream stream)
        {
            /* write the magic to the raw stream */
            stream.Write(BitConverter.GetBytes(MAGIC), 0, sizeof(uint));
            stream.Write(BitConverter.GetBytes(VERSION), 0, sizeof(uint));

            /* Write the magic */
            WriteUint(MAGIC);

            /* Write the version */
            WriteUint(VERSION);

            SerializeStatistics(data.Statistics);

            SerializeStackTraces(data.StackTraces);

            SerializeSQLStatements(data.SQLStatements);

            SerializeExecutionCalls(data.AllExecutionCalls);

            WriteLong(data.MaxCallDepth);

            ms.Seek(0, SeekOrigin.Begin);

            using (DeflateStream def = new DeflateStream(stream, CompressionLevel.Optimal))
            {
                ms.CopyTo(def);
            }
        }

        private void SerializeStackTraces(List<StackTraceEntry> stackTraces)
        {
            /* StackTraceEntry Layout 
             * InternalID - Uint
             * Message - string
             * Offender - string
             * StackTrac list count - uint
             * StackTrace - list of strings (in order)
             * Parent is Present - bool
             * Parent Call - InternalID
            */

            /* Write total number of stack trace entries */
            WriteUint(stackTraces.Count);

            foreach(StackTraceEntry trace in stackTraces)
            {
                WriteUint(trace.InternalID);
                WriteString(trace.Message);
                WriteString(trace.Offender);

                /* write number of trace lines */
                WriteUint(trace.StackTrace.Count);

                foreach (string t in trace.StackTrace)
                {
                    WriteString(t);
                }

                WriteBool(trace.Parent != null);

                if (trace.Parent != null)
                {
                    WriteUint(trace.Parent.InternalID);
                }
            }

        }

        private void SerializeStatistics(List<StatisticItem> stats)
        {
            /* StatisticsItem layout 
             * InternalId - Uint
             * Category - String
             * Label - String
             * Value - String
             * Tag Present - bool
             * Tag Type - byte
             * Tag - InternalId

            */
            /* Write out number of stat items */
            WriteUint(stats.Count);

            foreach (StatisticItem stat in stats)
            {
                WriteUint(stat.InternalID);
                WriteString(stat.Category);
                WriteString(stat.Label);
                WriteString(stat.Value);

                WriteBool(stat.Tag != null);
                if (stat.Tag != null)
                {
                    /* support for SQLStatement and Execution Call */
                    if (stat.Tag.GetType().Equals(typeof(SQLStatement)))
                    {
                        WriteByte(1);
                        WriteUint((stat.Tag as SQLStatement).InternalID);
                    }
                    else if (stat.Tag.GetType().Equals(typeof(ExecutionCall)))
                    {
                        WriteByte(2);
                        WriteUint((stat.Tag as ExecutionCall).InternalID);
                    }
                    else
                    {
                        // unsupported tag
                        WriteByte(0);
                    }
                }
            }
        }

        private void SerializeSQLStatements(List<SQLStatement> statements)
        {
            /* generate list of all SQL text, not binds or anything else, just the SQL Text */
            List<string> SQLText = new List<string>();
            List<string> ContextList = new List<string>();
            foreach (var s in statements)
            {
                if (SQLText.Contains(s.Statement) == false)
                {
                    SQLText.Add(s.Statement);
                }

                if (ContextList.Contains(s.Context) == false)
                {
                    ContextList.Add(s.Context);
                }
            }

            /* Write out all SQL Text */

            /* Write out total number */
            WriteUint(SQLText.Count);

            /* Write out each string */
            foreach (string s in SQLText)
            {
                WriteString(s);
            }

            /* Write out total number of Contexts */
            WriteUint(ContextList.Count);

            /* Write out each string */
            foreach (string s in ContextList)
            {
                WriteString(s);
            }

            /* Write total number of SQLStatement objects */
            WriteUint(statements.Count);

            /* Serialize each SQLStatement */
            foreach (SQLStatement s in statements)
            {
                /* serialized order of properties 
                 * Statement - IndexOf in SQLText list
                 * InternalId
                 * LineNumber
                 * Parent Present - bool
                 * ParentCall - InternalId
                 * FetchCount
                 * ExecTime
                 * FetchTime
                 * IsError
                 * SQLError - Inline (InternalID, Error Position, ReturnCode, Message)
                 * RCNumber
                 * Context - IndexOf in ContextList (add if not in the list yet)
                 * List of SQLBindValues - Inline (InternalID,Index,Type,Length,Value);
                 */

                WriteUint(SQLText.IndexOf(s.Statement));

                WriteUint(s.InternalID);
                WriteLong(s.LineNumber);

                /* write if ParentCall exists */
                WriteBool(s.ParentCall != null);

                if (s.ParentCall != null) { 
                    WriteUint(s.ParentCall.InternalID);
                }
                
                WriteInt(s.FetchCount);
                WriteDouble(s.ExecTime);
                WriteDouble(s.FetchTime);
                WriteBool(s.IsError);

                /* Write if ErrorInfo Exists */
                WriteBool(s.ErrorInfo != null);
                if (s.ErrorInfo != null)
                {
                    WriteUint(s.ErrorInfo.InternalID);
                    WriteUint(s.ErrorInfo.ErrorPosition);
                    WriteUint(s.ErrorInfo.ReturnCode);
                    WriteString(s.ErrorInfo.Message);
                }

                WriteUint(s.RCNumber);
                WriteUint(ContextList.IndexOf(s.Context));


                /* Write out number of SQL Executions */
                WriteUint(s.Executions.Count);
                foreach(var exec in s.Executions)
                {
                    WriteUint(exec.FetchCount);
                    WriteDouble(exec.ExecTime);
                    WriteDouble(exec.FetchTime);

                    /* Write out number of SQL Binds */
                    WriteUint(exec.BindValues.Count);
                    foreach (var bind in exec.BindValues)
                    {
                        /* write out each bind inline */
                        WriteUint(bind.InternalID);
                        WriteUint(bind.Index);
                        WriteUint(bind.Type);
                        WriteUint(bind.Length);
                        WriteString(bind.Value);
                    }
                }
                
            }


        }


        private void SerializeExecutionCalls(List<ExecutionCall> calls)
        {
            /* Generate Context List */
            List<string> ContextList = new List<string>();
            List<string> FunctionList = new List<string>();

            foreach (ExecutionCall call in calls)
            {
                if (ContextList.Contains(call.Context) == false)
                {
                    ContextList.Add(call.Context);
                }

                if (FunctionList.Contains(call.Function) == false)
                {
                    FunctionList.Add(call.Function);
                }
            }

            /* Write out Context List */
            WriteInt(ContextList.Count);

            foreach( var ctx in ContextList)
            {
                WriteString(ctx);
            }

            WriteInt(FunctionList.Count);

            foreach (var func in FunctionList)
            {
                WriteString(func);
            }

            WriteInt(calls.Count);

            foreach (ExecutionCall call in calls)
            {
                SerializeExecutionCall(ContextList, FunctionList, call);
            }
        }

        private void SerializeExecutionCall(List<string> ContextList, List<string> FunctionList, ExecutionCall call)
        {
            /* Parameters in order *
             * -InternalID
             * -SQLStatement Present - bool
             * -SQLStatement - InternalId
             * -StackTrace Present - bool
             * -StackTrace - InternalId
             * -PPCException Present - bool
             * -PPCException - inline (1,2,3)
             * -HasError - bool;
             * -IsError - bool;
             * -indentCount - uint
             * -Context - IndexOf in ContextList
             * Nest - String
             * Function - IndexOf in FunctionList
             * Duration (internal duration) - Double
             * Start Line - long
             * StopLine (internal stopline) - long
             * Execution Call Type
             * Call Parent Exists - bool
             * Execution Call Parent - InternalID
             */

            WriteUint(call.InternalID);
            WriteBool(call.SQLStatement != null);
            if (call.SQLStatement != null)
            {
                WriteUint(call.SQLStatement.InternalID);
            }

            WriteBool(call.StackTrace != null);
            if (call.StackTrace != null)
            {
                WriteUint(call.StackTrace.InternalID);
            }

            WriteBool(call.PPCException != null);
            if (call.PPCException != null)
            {
                WriteUint(call.PPCException.InternalID);
                WriteLong(call.PPCException.LineNumber);
                WriteString(call.PPCException.Message);
            }
            WriteBool(call.HasError);
            WriteBool(call.IsError);
            WriteInt(call.indentCount);
            WriteInt(ContextList.IndexOf(call.Context));
            WriteString(call.Nest);
            WriteInt(FunctionList.IndexOf(call.Function));
            WriteDouble(call.InternalDuration);
            WriteLong(call.StartLine);
            WriteLong(call.InternalStopLine);
            switch (call.Type)
            {
                case ExecutionCallType.CALL:
                    WriteByte(1);
                    break;
                case ExecutionCallType.EXTERNAL:
                    WriteByte(2);
                    break;
                case ExecutionCallType.NORMAL:
                    WriteByte(3);
                    break;
                case ExecutionCallType.SQL:
                    WriteByte(4);
                    break;
            }

            WriteBool(call.Parent != null);
            if (call.Parent != null)
            {
                WriteUint(call.Parent.InternalID);
            }
        }

        private void WriteByte(byte b)
        {
            ms.WriteByte(b);
        }

        private void WriteBool(bool b)
        {
            if (b)
            {
                ms.WriteByte(1);
            }
            else
            {
                ms.WriteByte(0);
            }
        }
        private void WriteLong(long l)
        {
            byte[] data = BitConverter.GetBytes(l);
            ms.Write(data, 0, data.Length);
        }

        private void WriteDouble(double d)
        {
            byte[] data = BitConverter.GetBytes(d);
            ms.Write(data, 0, data.Length);
        }

        private void WriteInt (int i)
        {
            byte[] data = BitConverter.GetBytes(i);
            ms.Write(data, 0, data.Length);
        }

        private void WriteUint(int i)
        {
            /* no need for negatives, cast to uint */
            WriteUint((uint)i);
        }
        private void WriteUint(uint u)
        {
            byte[] data = BitConverter.GetBytes(u);
            ms.Write(data, 0, data.Length);
        }

        private void WriteString(string str)
        {
            byte[] strLength = BitConverter.GetBytes(str.Length);
            byte[] strBytes = System.Text.Encoding.UTF8.GetBytes(str);

            if (str.Length < byte.MaxValue)
            {
                ms.WriteByte(1);
                ms.WriteByte(strLength[0]);
            } else if (str.Length < ushort.MaxValue)
            {
                ms.WriteByte(2);
                ms.Write(strLength,0,sizeof(ushort));
            } else if (str.Length < int.MaxValue)
            {
                ms.WriteByte(4);
                ms.Write(strLength, 0, sizeof(int));
            }
            ms.Write(strBytes, 0, strBytes.Length);
        }
        protected virtual void Dispose(bool all)
        {
            if (all)
            {
                ms.Dispose();
            }
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}

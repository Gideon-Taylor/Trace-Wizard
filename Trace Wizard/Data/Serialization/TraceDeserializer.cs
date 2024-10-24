using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TraceWizard.Data;

namespace TraceWizard.Data.Serialization
{
    public class TraceDeserializer : IDisposable
    {
        const uint MAGIC = 0x40631374;
        const uint VERSION = 0x01;
        MemoryStream ms = new MemoryStream();

        Dictionary<StatisticItem, uint> StatsToResolveSQL = new Dictionary<StatisticItem,uint>();
        Dictionary<StatisticItem, uint> StatsToResolveExec = new Dictionary<StatisticItem, uint>();

        Dictionary<StackTraceEntry, uint> StacksToResolve = new Dictionary<StackTraceEntry, uint>();

        Dictionary<SQLStatement, uint> SQLToResolveParent = new Dictionary<SQLStatement, uint>();

        Dictionary<ExecutionCall, uint> ExecToResolveSQL = new Dictionary<ExecutionCall, uint>();
        Dictionary<ExecutionCall, uint> ExecToResolveStackTrace = new Dictionary<ExecutionCall, uint>();

        TraceData Data = new TraceData();

        public static bool IsSerializedData(string filename)
        {
            using (FileStream fs = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.None))
            {
                byte[] magicBytes = new byte[sizeof(uint)];
                fs.Read(magicBytes, 0, sizeof(uint));
                //                byte[] versionBytes = new byte[sizeof(uint)];
                //fs.Read(versionBytes, 0, sizeof(uint));

                return BitConverter.ToUInt32(magicBytes,0) == MAGIC;
            }
        }

        public static bool IsSerializedVersionSupported(string filename)
        {
            using (FileStream fs = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.None))
            {
                byte[] magicBytes = new byte[sizeof(uint)];
                fs.Read(magicBytes, 0, sizeof(uint));
                byte[] versionBytes = new byte[sizeof(uint)];
                fs.Read(versionBytes, 0, sizeof(uint));

                return BitConverter.ToUInt32(versionBytes, 0) == VERSION;
            }
        }

        public TraceData DeserializeTraceData(Stream stream)
        {

            /* Read the magic/version off the raw stream */
            byte[] magicBytes = new byte[sizeof(uint)];
            stream.Read(magicBytes, 0, sizeof(uint));
            byte[] versionBytes = new byte[sizeof(uint)];
            stream.Read(versionBytes, 0, sizeof(uint));

            using (DeflateStream deflate = new DeflateStream(stream,CompressionMode.Decompress))
            {
                deflate.CopyTo(ms);
            }
            ms.Seek(0, SeekOrigin.Begin);
            /* decompressed file is in ms */

            /* Read Magic */
            var magic = ReadUint();

            if (magic != MAGIC)
            {
                throw new DeSerializationError("Invalid Magic");
            }

            var version = ReadUint();
            if (version != VERSION)
            {
                throw new DeSerializationError("Invalid Version - Built with " + version + " deserializing with " + VERSION);
            }

            DeserializeStatisticItems();

            DeserializeStackTraces();

            DeserializeSQLStatements();

            DeserializeExecutionCalls();

            ResolveDependencies();

            RebuildExecutionPath();

            RebuildSQLCollections();

            Data.MaxCallDepth = ReadLong();

            return Data;
        }



        private void DeserializeStatisticItems()
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

            /* get count of items */
            var totalCount = ReadUint();

            for (var x = 0; x < totalCount; x++)
            {
                /* read each item */
                StatisticItem item = new StatisticItem();
                item.InternalID = ReadUint();
                item.Category = ReadString();
                item.Label = ReadString();
                item.Value = ReadString();
                Data.Statistics.Add(item);
                bool tagPresent = ReadBool();

                if (tagPresent)
                {
                    byte tagType = ReadByte();
                    if (tagType == 1)
                    {
                        // SQL Object
                        StatsToResolveSQL.Add(item, ReadUint());
                    } else if (tagType == 2)
                    {
                        StatsToResolveExec.Add(item, ReadUint());
                    }
                }
            }
        }

        private void DeserializeStackTraces()
        {
            /* StackTraceEntry Layout 
             * InternalID - Uint
             * Message - string
             * Offender - string
             * StackTrace - list of strings (in order)
             * Parent is Present - bool
             * Parent Call - InternalID
            */

            /* get count of items */
            var totalCount = ReadUint();

            for (var x = 0; x < totalCount; x++)
            {
                /* read each item */
                StackTraceEntry item = new StackTraceEntry();
                Data.StackTraces.Add(item);

                item.InternalID = ReadUint();
                item.Message = ReadString();
                item.Offender = ReadString();

                var traceLineCount = ReadUint();
                for (var y = 0; y < traceLineCount; y++)
                {
                    item.StackTrace.Add(ReadString());
                }

                var parentPresent = ReadBool();
                if (parentPresent)
                {
                    StacksToResolve.Add(item, ReadUint());
                }
            }
        }

        private void DeserializeSQLStatements()
        {
            // whooo boy
            List<string> SQLText = new List<string>();
            List<string> ContextList = new List<string>();

            /* read in all SQLText */
            var sqlTextCount = ReadUint();

            for (var x = 0; x < sqlTextCount; x++)
            {
                SQLText.Add(ReadString());
            }

            /* read in all Context List */
            var contextCount = ReadUint();

            for (var x = 0; x < contextCount; x++)
            {
                ContextList.Add(ReadString());
            }

            var totalStatements = ReadUint();

            for (var x = 0; x < totalStatements; x++)
            {
                var statementIndex = ReadUint();
                SQLStatement item = new SQLStatement(SQLText[(int)statementIndex]);
                Data.SQLStatements.Add(item);

                item.InternalID = ReadUint();
                item.LineNumber = ReadLong();
                var parentPresent = ReadBool();

                if (parentPresent)
                {
                    SQLToResolveParent.Add(item, ReadUint());
                }
                item.FetchCount = ReadInt();
                item.ExecTime = ReadDouble();
                item.FetchTime = ReadDouble();
                item.IsError = ReadBool();

                var errorPresent = ReadBool();
                if (errorPresent)
                {
                    item.ErrorInfo = new SQLError();
                    item.ErrorInfo.InternalID = ReadUint();
                    item.ErrorInfo.ErrorPosition = (int)ReadUint();
                    item.ErrorInfo.ReturnCode = (int)ReadUint();
                    item.ErrorInfo.Message = ReadString();
                }

                item.RCNumber = (int)ReadUint();

                var contextIndex = ReadUint();
                item.Context = ContextList[(int)contextIndex];

                var bindCount = ReadUint();

                for (var y = 0; y < bindCount; y++)
                {
                    SQLBindValue bind = new SQLBindValue();
                    item.AddBindValue(bind);

                    bind.InternalID = ReadUint();
                    bind.Index = (int)ReadUint();
                    bind.Type = (int)ReadUint();
                    bind.Length = (int)ReadUint();
                    bind.Value = ReadString();
                }

            }

        }

        private void DeserializeExecutionCalls()
        {
            List<string> ContextList = new List<string>();
            List<string> FunctionList = new List<string>();

            var contextCount = ReadInt();
            for (var x = 0; x < contextCount; x++)
            {
                ContextList.Add(ReadString());
            }

            var functionCount = ReadInt();
            for (var x = 0; x < functionCount; x++)
            {
                FunctionList.Add(ReadString());
            }

            var totalCallCount = ReadInt();

            for (var x = 0; x < totalCallCount; x++)
            {
                DeserializeExecutionCall(ContextList, FunctionList);
            }

        }

        private void DeserializeExecutionCall(List<string> ContextList, List<string> FunctionList)
        {
            ExecutionCall call = new ExecutionCall();
            Data.AllExecutionCalls.Add(call);
            call.InternalID = ReadUint();

            var sqlStatementPresent = ReadBool();
            if (sqlStatementPresent)
            {
                var sqlId = ReadUint();
                ExecToResolveSQL.Add(call,sqlId);
            }

            var stackTracePresent = ReadBool();
            if (stackTracePresent)
            {
                var stackTraceId = ReadUint();
                ExecToResolveStackTrace.Add(call, stackTraceId);
            }

            var ppcExceptionPresent = ReadBool();
            if (ppcExceptionPresent)
            {
                call.PPCException = new PPCException();
                call.PPCException.InternalID = ReadUint();
                call.PPCException.LineNumber = ReadLong();
                call.PPCException.Message = ReadString();
            }

            call.HasError = ReadBool();
            call.IsError = ReadBool();
            call.indentCount = ReadInt();
            call.Context = ContextList[ReadInt()];
            call.Nest = ReadString();
            call.Function = FunctionList[ReadInt()];
            call.Duration = ReadDouble();
            call.StartLine = ReadLong();
            call.StopLine = ReadLong();

            var callType = ReadByte();
            switch(callType)
            {
                case 1:
                    call.Type = ExecutionCallType.CALL;
                    break;
                case 2:
                    call.Type = ExecutionCallType.EXTERNAL;
                    break;
                case 3:
                    call.Type = ExecutionCallType.NORMAL;
                    break;
                case 4:
                    call.Type = ExecutionCallType.SQL;
                    break;
            }

            var parentPresent = ReadBool();
            if (parentPresent)
            {
                //TODO: Can we shortcut this and look starting backwards for our parent?
                var parentID = ReadUint();
                for (var y = Data.AllExecutionCalls.Count - 2; y >= 0; y--)
                {
                    if (Data.AllExecutionCalls[y].InternalID.Equals(parentID))
                    {
                        Data.AllExecutionCalls[y].Children.Add(call);
                        call.Parent = Data.AllExecutionCalls[y];
                        break;
                    }
                }
            }

        }

        private void ResolveDependencies()
        {
            /* Resolve Statistics w/ SQL Tags */
            foreach (KeyValuePair<StatisticItem,uint>keyPair in StatsToResolveSQL)
            {
                /* find the SQLStatement w/ matching internal id */
                var targetId = keyPair.Value;

                for (var x = 0; x < Data.SQLStatements.Count; x++)
                {
                    if (Data.SQLStatements[x].InternalID == targetId)
                    {
                        keyPair.Key.Tag = Data.SQLStatements[x];
                        break;
                    }
                }
            }

            /* Resolve Statistics w/ Exec Tags */
            foreach (KeyValuePair<StatisticItem, uint> keyPair in StatsToResolveExec)
            {
                /* find the SQLStatement w/ matching internal id */
                var targetId = keyPair.Value;

                for (var x = 0; x < Data.AllExecutionCalls.Count; x++)
                {
                    if (Data.AllExecutionCalls[x].InternalID == targetId)
                    {
                        keyPair.Key.Tag = Data.AllExecutionCalls[x];
                        break;
                    }
                }
            }

            /* Resolve StackTraceEntry dependencies */
            foreach (KeyValuePair<StackTraceEntry, uint> keyPair in StacksToResolve)
            {
                var targetId = keyPair.Value;
                for (var x = 0; x < Data.AllExecutionCalls.Count; x++)
                {
                    if (Data.AllExecutionCalls[x].InternalID == targetId)
                    {
                        keyPair.Key.Parent = Data.AllExecutionCalls[x];
                        break;
                    }
                }
            }

            /* Resolve SQL Parent Calls */
            foreach (KeyValuePair<SQLStatement, uint> keyPair in SQLToResolveParent)
            {
                var targetId = keyPair.Value;
                for (var x = 0; x < Data.AllExecutionCalls.Count; x++)
                {
                    if (Data.AllExecutionCalls[x].InternalID == targetId)
                    {
                        keyPair.Key.ParentCall = Data.AllExecutionCalls[x];
                        break;
                    }
                }
            }

            /* Resolve Exec Calls w/ SQL */
            foreach (KeyValuePair<ExecutionCall, uint> keyPair in ExecToResolveSQL)
            {
                var targetId = keyPair.Value;
                for (var x = 0; x < Data.SQLStatements.Count; x++)
                {
                    if (Data.SQLStatements[x].InternalID == targetId)
                    {
                        keyPair.Key.SQLStatement = Data.SQLStatements[x];
                        break;
                    }
                }
            }

            /* Resolve Exec Calls w/ StackTraces */
            foreach (KeyValuePair<ExecutionCall, uint> keyPair in ExecToResolveSQL)
            {
                var targetId = keyPair.Value;
                for (var x = 0; x < Data.StackTraces.Count; x++)
                {
                    if (Data.StackTraces[x].InternalID == targetId)
                    {
                        keyPair.Key.StackTrace = Data.StackTraces[x];
                    }
                }
            }
        }

        private void RebuildExecutionPath()
        {
            foreach (ExecutionCall call in Data.AllExecutionCalls)
            {
                if (call.Parent == null)
                {
                    Data.ExecutionPath.Add(call);
                }
            }
        }

        private void RebuildSQLCollections()
        {
            /* Group them all by Where */
            var sqlByWhereList = Data.SQLByWhere;

            var byWheres = Data.SQLStatements.Where(p => p.Type != SQLType.INSERT).GroupBy(p => p.WhereClause).Select(g => new SQLByWhere { NumberOfCalls = g.Count(), TotalTime = g.Sum(i => i.Duration), WhereClause = g.Key, HasError = g.Count(p => p.IsError) > 0 ? true : false });
            foreach (var byW in byWheres)
            {
                sqlByWhereList.Add(byW);
            }

            var sqlByFromList = Data.SQLByFrom;

            var byFroms = Data.SQLStatements.Where(p => p.Type == SQLType.SELECT || p.Type == SQLType.DELETE).GroupBy(p => p.FromClause).Select(g => new SQLByFrom { NumberOfCalls = g.Count(), TotalTime = g.Sum(i => i.Duration), FromClause = g.Key, HasError = g.Count(p => p.IsError) > 0 ? true : false });
            foreach (var byF in byFroms)
            {
                sqlByFromList.Add(byF);
            }
        }

        private byte ReadByte()
        {
            return (byte)ms.ReadByte();
        }

        private bool ReadBool()
        {
            var b = ReadByte();
            if (b == 1)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        private long ReadLong()
        {
            byte[] data = new byte[sizeof(long)];
            ms.Read(data, 0, sizeof(long));
            return BitConverter.ToInt64(data, 0);
        }

        private double ReadDouble()
        {
            byte[] data = new byte[sizeof(double)];
            ms.Read(data, 0, sizeof(double));
            return BitConverter.ToDouble(data, 0);
        }

        private int ReadInt()
        {
            byte[] data = new byte[sizeof(int)];
            ms.Read(data, 0, sizeof(int));

            return BitConverter.ToInt32(data, 0);
        }

        private uint ReadUint()
        {
            byte[] data = new byte[sizeof(int)];
            ms.Read(data, 0, sizeof(int));

            return BitConverter.ToUInt32(data, 0);
        }
        private ushort ReadShort()
        {
            byte[] data = new byte[sizeof(short)];
            ms.Read(data, 0, sizeof(short));

            return BitConverter.ToUInt16(data, 0);
        }
        private string ReadString()
        {
            
            int stringLength = 0;
            int lenByteCount = ReadByte();
            if (lenByteCount == 1)
            {
                stringLength = ReadByte();
            } else if (lenByteCount == 2)
            {
                stringLength = ReadShort();
            } else if (lenByteCount == 4)
            {
            stringLength = ReadInt();
            }
            byte[] strBytes = new byte[stringLength];
            ms.Read(strBytes, 0, stringLength);

            return System.Text.Encoding.UTF8.GetString(strBytes);
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

    [Serializable]
    public class DeSerializationError : Exception
    {
        public DeSerializationError(String msg) : base(msg)
        {
        }

        public override string ToString()
        {
            return Message;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TraceWizard.Data
{
    public class VariableBundle
    {
        public BundleType Type;
        public long StartLine;
        public string Context;
        public List<Variable> Variables;
    }

    public enum BundleType
    {
        GLOBAL_SERIALIZED, GLOBAL_DESERIALIZED, COMPONENT_SERIALIZED, COMPONENT_DESERIALIZED
    }

    public class Variable
    {
        public string Name;
        public string Type;
        public string Value;
        public bool IsObject;
        public bool IsArray;
        public Variable Parent;
        public List<Variable> ChildValues;
        internal int Depth;
    }
}

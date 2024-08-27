using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TraceWizard.Data
{
    public class ComponentBuffer
    {
        public BufferType Type;
    }


    public enum BufferType
    {
        SEARCH_RESULTS, INIT, BEFORE_SERVICE, AFTER_SERVICE, AFTER_SCROLLSELECT, AFTER_MODAL, AFTER_ROW_INSERT, BEFORE_SAVE
    }
}

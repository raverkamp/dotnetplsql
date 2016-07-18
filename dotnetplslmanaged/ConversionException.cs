using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace spinat.dotnetplslmanaged
{
    public class ConversionException : Exception
    {

        public ConversionException(String msg)
            : base(msg)
        {

        }
    }
}

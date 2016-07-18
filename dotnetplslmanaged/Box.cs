using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace spinat.dotnetplslmanaged
{
    public class Box
    {
        private Object value;

        public Object Value
        {
            set
            {
                this.value = value;
            }
            get
            {
                return this.value;
            }
        }

        public Box()
        {
            this.value = null;
        }

        public Box(Object x)
        {
            this.value = x;
        }

    }

}

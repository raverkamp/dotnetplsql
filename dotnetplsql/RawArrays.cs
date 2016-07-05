using System;
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;
namespace spinat.dotnetplsql {
	
public class SimpleRawArray : IOracleCustomType, INullable
{
    [OracleArrayMapping()]
    public byte[][] Array;
 
    private OracleUdtStatus[] m_statusArray;
    
    public OracleUdtStatus[] StatusArray
    {
        get
        {
            return this.m_statusArray;
        }
        set
        {
            this.m_statusArray = value;
        }
    }

    private bool m_bIsNull = false;

    public bool IsNull
    {
        get
        {
          return m_bIsNull;
        }
    }

    public static SimpleRawArray Null
    {
        get
        {
            SimpleRawArray obj = new SimpleRawArray();
            obj.m_bIsNull = true;
            return obj;
        }
    }

    public void ToCustomObject(Oracle.DataAccess.Client.OracleConnection con, IntPtr pUdt)
    {
        object objectStatusArray = null;
        //var x = OracleUdt.GetValue(con, pUdt, 0, out objectStatusArray);
        Array = (byte[][])OracleUdt.GetValue(con, pUdt, 0, out objectStatusArray);
        m_statusArray = (OracleUdtStatus[])objectStatusArray;
    }

    public void FromCustomObject(OracleConnection con, IntPtr pUdt)
    {
        OracleUdt.SetValue(con, pUdt, 0, Array, m_statusArray);
    }

    public override string ToString()
    {
        if (m_bIsNull)
            return "SimpleRawArray.Null";
        else
        {
            string rtnstr = String.Empty;
            if (m_statusArray[0] == OracleUdtStatus.Null)
                rtnstr = "NULL";
            else
                rtnstr = Array.GetValue(0).ToString();
            for (int i = 1; i < m_statusArray.Length; i++)
            {
                if (m_statusArray[i] == OracleUdtStatus.Null)
                    rtnstr += "," + "NULL";
                else
                    rtnstr += "," + Array.GetValue(i).ToString();
            }
            return "SimpleRawArray(" + rtnstr + ")";
        }
    }
}

/* SimpleVarrayFactory Class
**   An instance of the SimpleVarrayFactory class is used to create 
**   SimpleVarray objects
*/
[OracleCustomTypeMapping("ROLAND.RAW_ARRAY")]
public class SimpleRawArrayFactory : IOracleCustomTypeFactory,   IOracleArrayTypeFactory
{
    // IOracleCustomTypeFactory
    public IOracleCustomType CreateObject()
    {
        return new SimpleRawArray();
    }

    // IOracleArrayTypeFactory Inteface
    public Array CreateArray(int numElems)
    {
    	return new byte[numElems][];
    }

    public Array CreateStatusArray(int numElems)
    {
        // CreateStatusArray may return null if null status information 
        // is not required.
        return new OracleUdtStatus[numElems];
    }
}
}
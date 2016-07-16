using System;
using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using spinat.dotnetplslmanaged;
using NUnit.Framework;

namespace spinat.dotnetplsqltests
{


public class ErrorTestManaged {

    public ErrorTestManaged() {
    }

   OracleConnection connection;

        [OneTimeSetUp]
        public void setUp()
        {

            var b = new OracleConnectionStringBuilder();
            b.UserID = "roland";
            b.Password = "roland";
            b.DataSource = "ltrav01";

            var c = new OracleConnection();
            c.ConnectionString = b.ConnectionString;
            c.Open();
            this.connection = c;

            Dictionary<String, String> a = TestUtil.LoadSnippets("snippets.txt");

            Ddl.call(connection, a["p1_spec"]);
            Ddl.call(connection, a["p1_body"]);
            Ddl.call(connection, a["proc1"]);

            Ddl.createType(connection, "create type number_array as table of number;");
            Ddl.createType(connection, "create type varchar2_array as table of varchar2(32767);");
            Ddl.createType(connection, "create type date_array as table of date;");
            Ddl.createType(connection, "create type raw_array as table of raw(32767);");
        }

  [Test]
    public void testRaiseApplicationError() {
        ProcedureCaller p = new ProcedureCaller(connection);
        var a = new Dictionary<String, Object>();
        a["ERRNUM"] =  -20012;
        a["TXT"] =  "schlimmer fehler";
        // the output looks OK, maybe there is nothing to be done
        try {
            var res = p.call("p1.raise_error", a);
            System.Console.WriteLine(res);
        } catch (Exception ex) {
            System.Console.WriteLine(ex);
        }
    }
	
    [Test]
    public void testToLargeVarchar2()  {
        Type cl = null;
        ProcedureCaller p = new ProcedureCaller(connection);
        var a = new Dictionary<String,Object> ();
       var r =  new Dictionary<String,Object>();
        r["X"] =  1;
        r["Y"] = new String(new char[201]);
        r["Z"] = null;
        a["A"] =  r;
        try {
            var res = p.call("p1.p2", a);
        } catch (Exception ex) {
            System.Console.WriteLine(ex);
            cl = ex.GetType();
        }
        Assert.True(cl.Name.Contains("ConversionException"));
    }
	
    [Test]
    public void testSlotMissing()  {
        Type cl = null;
        ProcedureCaller p = new ProcedureCaller(connection);
        var a = new Dictionary<String, Object>();
        var r = new Dictionary<String, Object>();
        r["X"] =  1;
        //r.put("Y", new String(new char[20]));
        r["Z"] =  null;
        a["A"] = r;
        try {
           var res = p.call("p1.p2", a);
        } catch (Exception ex) {
            System.Console.WriteLine(ex);
            cl = ex.GetType();
        }
        Assert.True(cl.Name.Contains("ConversionException"));
    }

     
    [Test]
    public void testParamMissing()  {
        Type cl = null;
        ProcedureCaller p = new ProcedureCaller(connection);
        var a = new  Dictionary<String, Object>();
        try {
            var res = p.call("p1.p2", a);
        } catch (Exception ex) {
            System.Console.WriteLine(ex);
            cl = ex.GetType();
        }
        Assert.True(cl.Name.Contains("ConversionException"));
    }
	
}

}
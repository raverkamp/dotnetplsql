using System;
using System.Collections.Generic;
using Oracle.DataAccess.Client;
using spinat.dotnetplsql;

namespace spinat.dotnetplsqltests {



public class ExampleTest {

    public ExampleTest(OracleConnection c) {
			this.connection = c;
			
    }

    readonly OracleConnection connection;
  
    public void example1()  {
    	OracleCommand c = connection.CreateCommand();
    	c.CommandText =
        "create or replace package example1 as \n"
                + " type rec is record (x number,y varchar2(200),z date);\n"
                + " type array is table of rec;\n"
                + "procedure p(a in array,b out array);\n"
                + "end;";
    	c.ExecuteNonQuery();
        c.CommandText ="create or replace package body example1 as\n"
                + " procedure p(a in array,b out array) is\n"
                + " begin\n"
                + " b:=a;\n"
                + " end;\n"
                + " end;";
    	c.ExecuteNonQuery();
    	
        ProcedureCaller procCaller = new ProcedureCaller(connection);
        // the input to he procedure example.p1 is a table of records
        // the variable al is the table
        List<Dictionary<String, Object>> al = new List<Dictionary<String, Object>>();
        for (int i = 0; i < 10; i++) {
            // and the variable m is the record, in Java this is a Map<String,Object>
            Dictionary<String, Object> m = new  Dictionary<String, Object>();
            m["X"] = i;
            m["Y"] =  "text-" + i;
            m["Z"] = DateTime.Now.AddDays(i); // simple value?
            al.Add(m);
        }
        // arguments are transferd by name
        Dictionary<String, Object> args = new Dictionary<String, Object>();
        // the argument names are case sensitive
        args["A"] = al;
        Dictionary<String, Object> result = procCaller.call("example1.p", args);
        // all out paramters are packed into a Map<String,Object> and returned
        foreach (Object o in (List< Object>) result["B"]) {
            var d = (Dictionary<String, Object>)o;
            foreach (String k in d.Keys)
            {
                Console.Write(" key=" + k + " value=" + d[k]);
            }
        	Console.WriteLine("");
        }

    }
}
}

using System;
using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using spinat.dotnetplslmanaged;
using NUnit.Framework;
using System.Text;

namespace spinat.dotnetplsqltests
{

     [TestFixture]
    public class PerfTestManaged
    {

        public PerfTestManaged()
        {
        }

        //@BeforeClass
        //public static void setUpClass() {
        //}

        //@AfterClass
        //public static void tearDownClass() {
        //}

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
        //@After
        //public void tearDown() {
        //}

        public void test3Base(int size)
        {
            var ar = new Dictionary<String, Object>();
            var l = new List<Dictionary<String, Object>>();
            for (int i = 0; i < size; i++)
            {
                var a = new Dictionary<String, Object>();
                a["X"]=  new Decimal(i);
                a["Y"] = "x" + i;
                a["Z"] =  new DateTime();
                l.Add(a);
            }
            ar["A"] =  l;
            var res = new ProcedureCaller(connection).call("P1.P3", ar);
            var l2 = (List<Object>)res["B"];
            for (int i = 0; i < l.Count; i++)
            {
                var m = (Dictionary<String, Object>)l[i];
                var m2 = (Dictionary<String, Object>)l2[i];
                Assert.True(m2["X"].Equals(((Decimal)m["X"])+new decimal(1))
                        && m2["Y"].Equals("" + m["Y"] + m["Y"]));
            }
        }

        void perfTest(int k)
        {
            int n = 1;
            //DbmsOutput.enableDbmsOutput(connection, 0);
            for (int i = 0; i < k; i++)
            {
                long l = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                test3Base(n);

                l = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond - l;
                System.Console.WriteLine("" + n + ": " + l);
                //for (String s : DbmsOutput.fetchDbmsOutput(connection)) {
                //    System.out.println("  " + s);
                //}

                n = n * 2;
            }
        }

        void sizeTest(int rec_size, int args_size)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("create or replace package p2 as\n")
                    .Append("type r is record (\n");
            for (int i = 0; i < rec_size; i++)
            {
                if (i > 0)
                {
                    sb.Append(",\n");
                }
                sb.Append("x" + i + " number,y" + i + " varchar2(200),z" + i + " date");
            }
            sb.Append(");\n");
            sb.Append("procedure p(");
            for (int i = 0; i < args_size; i++)
            {
                if (i > 0)
                {
                    sb.Append(",\n");
                }
                sb.Append("in" + i + " r,out" + i + " out r\n");

            }
            sb.Append(");\n");
            sb.Append("end;\n");
            Ddl.call(connection, sb.ToString());
            sb = new StringBuilder();
            sb.Append("create or replace package body p2 as\n");
            sb.Append("procedure p(");
            for (int i = 0; i < args_size; i++)
            {
                if (i > 0)
                {
                    sb.Append(",\n");
                }
                sb.Append("in" + i + " r,out" + i + " out r\n");
            }
            sb.Append(") is\n");
            sb.Append("begin\n");
            for (int i = 0; i < args_size; i++)
            {
                sb.Append("out" + i + ":=in" + i + ";\n");
            }
            sb.Append("end;\n");
            sb.Append("end;\n");
            Ddl.call(connection, sb.ToString());
            var args = new Dictionary<String, Object>();
            var m = new Dictionary<String, Object>();
            for (int i = 0; i < rec_size; i++)
            {
                m["X" + i] = i * 8.78;
                m["Y" + i] =  "String" + i;
                m["Z" + i] = DateTime.Now;
            }
            for (int i = 0; i < args_size; i++)
            {
                args["IN" + i] = m;
            }
            var res = new ProcedureCaller(connection).call("P2.P", args);
            System.Console.WriteLine(res);
        }

        void varcharSizeTest(int vsize, int asize)
        {
            var a = new List<Object>();
            for (int i = 0; i < asize; i++)
            {
                StringBuilder sb = new StringBuilder();
                int j = 0;
                while (sb.Length < vsize)
                {
                    sb.Append("x" + j);
                    j++;
                }
                a.Add(sb.ToString().Substring(0, vsize));
            }
            var args = new Dictionary<String, Object>();
            args["A"] = a;
          var res = new ProcedureCaller(connection).call("P1.P5", args);
           var b = (List<Object>)res["B"];
            for (int i = 0; i < Math.Max(a.Count, b.Count); i++)
            {
                Assert.AreEqual(a[i], b[i]);
            }
        }

        [Test]
        public void varcharSizeTest_32766_100()
        {
            varcharSizeTest(32766, 1);
        }

        [Test]
        public void sizeTest_30_100()
        {
            sizeTest(3, 1);
        }

        [Test]
        public void perfTest_1000()
        {
            perfTest(6);
        }

    }
}

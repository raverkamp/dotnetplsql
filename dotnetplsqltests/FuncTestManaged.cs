using System;
using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using spinat.dotnetplslmanaged;
using NUnit.Framework;
using System.Data;

namespace spinat.dotnetplsqltests
{
    [TestFixture]
    public class FuncTestManaged
    {

        public FuncTestManaged()
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

        [Test]
        public void t1()
        {
            var ar = new Dictionary<String, Object>();
            ar["XI"] = 12;
            ar["YI"] = "x";
            ar["ZI"] = DateTime.Now;
            var res = new ProcedureCaller(connection).call("P1.P", ar);
            System.Console.WriteLine(res);
            Assert.True(res["XO"].Equals(new Decimal(13)) && res["YO"].Equals("xx"));
        }

        [Test]
        public void t1p()
        {

            ProcedureCaller p = new ProcedureCaller(connection);
            var xo = new Box<Object>();
            var yo = new Box<Object>();
            var zo = new Box<Object>();

            p.callPositional("P1.P", 12, "x", DateTime.Now, xo, yo, zo);
            Assert.AreEqual(xo.value, new decimal(13));
            Assert.AreEqual(yo.value, "xx");
        }

        [Test]
        public void test2()
        {
            Dictionary<String, Object> ar = new Dictionary<String, Object>();
            var a = new Dictionary<String, Object>();
            a["X"] = 12;
            a["Y"] = "x";
            a["Z"] = DateTime.Now;
            ar["A"] = a;
            Dictionary<String, Object> res = new ProcedureCaller(connection).call("P1.P2", ar);
            Console.WriteLine(res);
            Dictionary<String, Object> m = (Dictionary<String, Object>)res["B"];
            Assert.True(m["X"].Equals(new Decimal(13)) && m["Y"].Equals("xx"));
        }

        [Test]
        public void test2p()
        {
            var a = new Dictionary<String, Object>();
            a["X"] = 12;
            a["Y"] = "x";
            a["Z"] = DateTime.Now;
            ProcedureCaller p = new ProcedureCaller(connection);
            Box<Object> b = new Box<Object>();
            p.callPositional("P1.P2", a, b);
            Dictionary<String, Object> m = (Dictionary<String, Object>)b.value;
            Assert.AreEqual(m["X"], new Decimal(13));
            Assert.AreEqual(m["Y"], "xx");
        }

        public void test3Base(int size)
        {
            Dictionary<String, Object> ar = new Dictionary<String, Object>();
            List<Dictionary<String, Object>> l = new List<Dictionary<String, Object>>();
            for (int i = 0; i < size; i++)
            {
                var a = new Dictionary<String, Object>();
                a["X"] = new Decimal(i);
                a["Y"] = "x" + i;
                a["Z"] = DateTime.Now;
                l.Add(a);
            }
            ar["A"] = l;
            Dictionary<String, Object> res = new ProcedureCaller(connection).call("P1.P3", ar);
            var l2 = (System.Collections.IList)res["B"];
            for (int i = 0; i < l.Count; i++)
            {
                Dictionary<String, Object> m = l[i];
                Dictionary<String, Object> m2 = (Dictionary<String, Object>)l2[i];
                Assert.True(m2["X"].Equals(((Decimal)m["X"]) + new Decimal(1)));
                Assert.True(m2["Y"].Equals("" + m["Y"] + m["Y"]));
            }
        }

        [Test]
        public void test3()
        {
            test3Base(10);
        }

        [Test]
        public void test4()
        {
            var ar = new Dictionary<String, Object>();
            var l0 = new List<Object>();
            for (int j = 0; j < 3; j++)
            {
                var l = new List<Dictionary<String, Object>>();
                for (int i = 0; i < 2; i++)
                {
                    var a = new Dictionary<String, Object>();
                    a["X"] = new Decimal(i);
                    a["Y"] = "x" + i;
                    a["Z"] = new DateTime(2013, 5, 1);
                    l.Add(a);
                }
                l0.Add(l);
            }
            ar["A"] = l0;
            Dictionary<String, Object> res = new ProcedureCaller(connection).call("P1.P4", ar);
            System.Console.WriteLine(res);
            var l2 = (System.Collections.IList)res["A"];
            for (int i = 0; i < l2.Count; i++)
            {
                var x = (System.Collections.IList)l2[i];
                var y = (System.Collections.IList)l0[i];
                for (int j = 0; j < x.Count; j++)
                {
                    var m1 = (Dictionary<String, Object>)x[j];
                    var m2 = (Dictionary<String, Object>)y[j];
                    Assert.True(m1["X"].Equals(m2["X"]));
                    Assert.True(m1["Y"].Equals(m2["Y"]));
                    Assert.True(m1["Z"].Equals(m2["Z"]));
                }
            }
        }

        [Test]
        public void test5()
        {
            var ar = new Dictionary<String, Object>();
            var a = new Dictionary<String, Object>();
            a["X"] = null;
            a["Y"] = null;
            a["Z"] = null;
            ar["A"] = a;
            var res = new ProcedureCaller(connection).call("P1.P2", ar);
            System.Console.WriteLine(res);
            var m = (Dictionary<String, Object>)res["B"];
            Assert.True(m["X"] == null);
            Assert.True(m["Y"] == null);
            Assert.True(m["Z"] != null);
        }

        [Test]
        public void test5p()
        {
            var a = new Dictionary<String, Object>();
            a["X"] = null;
            a["Y"] = null;
            a["Z"] = null;
            Box<Object> b = new Box<Object>();
            new ProcedureCaller(connection).callPositional("P1.P2", a, b);
            var m = (Dictionary<String, Object>)b.value;
            Assert.IsNull(m["X"]);
            Assert.IsNull(m["Y"]);
        }

        [Test]
        public void test6()
        {
            var ar = new Dictionary<String, Object>();
            var l0 = new List<Object>();
            for (int j = 0; j < 3; j++)
            {
                l0.Add(null);
            }
            ar["A"] = l0;
            var res = new ProcedureCaller(connection).call("P1.P4", ar);
            Console.WriteLine(res);
            var l2 = (System.Collections.IList)res["A"];
            for (int i = 0; i < l2.Count; i++)
            {
                Assert.True(l2[i] == null);
            }
        }

        [Test]
        public void test7()
        {
            var a = new Dictionary<String, Object>();
            a["X"] = 23;
            a["Y"] = "roland";
            a["Z"] = DateTime.Now;
            var m = new ProcedureCaller(connection).call("proc1", a);
        }

        [Test]
        public void test8()
        {
            var a = new Dictionary<String, Object>();
            var m = new ProcedureCaller(connection).call("p1.p6", a);
        }

        [Test]
        public void test9()
        {
            var a = new Dictionary<String, Object>();
            a["A"] = -123;
            a["B"] = "rote gruetze";
            var dat = new DateTime(2014, 12, 3, 23, 45, 1, 0);
            a["C"] = dat;
            var m = new ProcedureCaller(connection).call("p1.f7", a);
            var r = (Dictionary<String, Object>)m["RETURN"];
            Assert.True(r["X"].Equals(new Decimal(-123)));
            Assert.True(r["Y"].Equals("rote gruetze"));
            Assert.True(r["Z"].Equals(dat));
        }

        [Test]
        public void test10()
        {
            var a = new Dictionary<String, Object>();
            {
                a["X"] = true;
                var m = new ProcedureCaller(connection).call("p1.p8", a);
                var y = (bool)(m["Y"]);
                Assert.True(!y);
            }
            {
                a["X"] = false;
                var m = new ProcedureCaller(connection).call("p1.p8", a);
                var y = (bool)(m["Y"]);
                Assert.True(y);
            }
            {
                a["X"] = null;
                var m = new ProcedureCaller(connection).call("p1.p8", a);
                var y = (bool?)(m["Y"]);
                Assert.True(!y.HasValue);
            }
        }

        [Test]
        public void test11()
        {
            var a = new Dictionary<String, Object>();
            {
                a["X1"] = 1;
                a["X2"] = 19;
                var m = new ProcedureCaller(connection).call("p1.p9", a);
                var y1 = (Decimal)m["Y1"];
                Assert.True(y1.Equals(new Decimal(-1)));
                var y2 = (Decimal)m["Y2"];
                Assert.True(y2.Equals(new Decimal(29)));
            }
        }

        // procedure p5 takes a table is argument and return it in its
        // out argument
        //[Test]
        //public void testTableNull()
        //{
        //    Box<Object> b = new Box<Object>();
        //    ProcedureCaller p = new ProcedureCaller(connection);
        //    p.callPositional("p1.p5", null, b);
        //    Assert.Null(b.value);

        //    List<String> l = new List<String>();
        //    p.callPositional("p1.p5", l, b);
        //    Assert.NotNull(b.value);
        //    Assert.AreEqual(0, ((List<Object>)b.value).Count);
        //}

        // test various methods to write a strored procedure
        [Test]
        public void testName1()
        {
            var a = new Dictionary<String, Object>();
            ProcedureCaller p = new ProcedureCaller(connection);
            Dictionary<String, Object> res;
            res = p.call("p1.no_args", a);
            res = p.call("\"P1\".no_args", a);
            res = p.call("\"P1\".\"NO_ARGS\"", a);
            res = p.call("p1.\"NO_ARGS\"", a);
            res = p.call("p1.\"NO_ARGS\"", a);
        }

        [Test]
        public void testName2()
        {
            var a = new Dictionary<String, Object>();
            ProcedureCaller p = new ProcedureCaller(connection);
            Dictionary<String, Object> res;
            Exception ex = null;
            try
            {
                res = p.call("p1.this_proc_does_not_exist", a);
            }
            catch (Exception exe)
            {
                ex = exe;
            }
            Assert.NotNull(ex);
            Assert.True(ex is ApplicationException);
            Assert.True(ex.Message.Contains("procedure in package does not exist or object is not valid"));
        }


        [Test]
        public void TestSysRefCursor()
        {
            ProcedureCaller p = new ProcedureCaller(connection);
            Box<Object> b = new Box<Object>();
            var dat = new DateTime(2001, 12, 1);
            p.callPositional("p1.pcursor1", 17, "xyz", dat, b);
            var l = (List<Dictionary<String, Object>>)b.value;
            Assert.AreEqual(l.Count, 2);
            var r2 = l[1];
            Assert.AreEqual(r2["A"], "xyz");
            Assert.AreEqual(r2["B"], new decimal(17));
            Assert.AreEqual(r2["C"], dat);
        }
        [Test]
        public void TestSysRefCursorAsDataTable()
        {
            ProcedureCaller p = new ProcedureCaller(connection);
            p.ReturnRefCursorAsDataTable = true;
            Box<Object> b = new Box<Object>();
            var dat = new DateTime(2001, 12, 1);
            p.callPositional("p1.pcursor1", 17, "xyz", dat, b);
            var tab = (System.Data.DataTable)b.value;
            Assert.AreEqual(tab.Rows.Count, 2);
            var r2 = tab.Rows[1];
            Assert.AreEqual(r2["A"], "xyz");
            Assert.AreEqual(r2["B"], new decimal(17));
            Assert.AreEqual(r2["C"], dat);
        }

        [Test]
        public void TestRefCursor()
        {
            ProcedureCaller p = new ProcedureCaller(connection);
            Box<Object> b = new Box<Object>();
            DateTime dat = new DateTime(2001, 12, 1);
            p.callPositional("p1.pcursor2", 17, "xyz", dat, b);
            var l = (List<Dictionary<String, Object>>)b.value;
            Assert.AreEqual(l.Count, 2);
            var r2 = l[1];
            Assert.AreEqual(r2["V"], "xyz");
            Assert.AreEqual(r2["N"], new Decimal(17));
            Assert.AreEqual(r2["D"], dat);
        }

        [Test]
        public void TestRefCursorAsDataTable()
        {
            ProcedureCaller p = new ProcedureCaller(connection);
            p.ReturnRefCursorAsDataTable = true; 
            Box<Object> b = new Box<Object>();
            DateTime dat = new DateTime(2001, 12, 1);
            p.callPositional("p1.pcursor2", 17, "xyz", dat, b);
            var tab = (DataTable)b.value;
            Assert.AreEqual(tab.Rows.Count, 2);
            var r2 = tab.Rows[1];
            Assert.AreEqual(r2["V"], "xyz");
            Assert.AreEqual(r2["N"], new Decimal(17));
            Assert.AreEqual(r2["D"], dat);
        }

        [Test]
        public void TestRefCursor3()
        {
            ProcedureCaller p = new ProcedureCaller(connection);
            Box<Object> b = new Box<Object>();
            Exception ex = null;
            try
            {
                p.callPositional("p1.pcursor3", b);
            }
            catch (Exception exe)
            {
                ex = exe;
            }
            Assert.True(ex != null && ex is ApplicationException);
            Assert.True(ex.Message.Contains("%rowtype"));
        }

        [Test]
        public void TestIndexBy()
        {
            int n = 20;
            ProcedureCaller p = new ProcedureCaller(connection);
            var args = new Dictionary<String, Object>();
            var ai = new SortedDictionary<String, Object>();
            for (int i = 0; i < n; i++)
            {
                ai["k" + i] = "v" + i;
            }
            args["AI"] = ai;
            var bi = new SortedDictionary<int, Object>();
            for (int i = 0; i < n; i++)
            {
                bi[i] = "v" + i;
            }
            args["BI"] = bi;
            var res = p.call("p1.pindex_tab", args);
            var ao = (SortedDictionary<String, Object>)res["AO"];
            var bo = (SortedDictionary<int, Object>)res["BO"];
            Assert.AreEqual(ao.Count, n);
            for (int i = 0; i < n; i++)
            {
                Assert.IsTrue(ao["xk" + i].Equals(ai["k" + i] + "y"));
            }

            Assert.AreEqual(bo.Count, n);
            for (int i = 0; i < n; i++)
            {
                Assert.True(bo[2 * i].Equals(bi[i] + "y"));
            }
        }

        [Test]
        public void TestIndexBy2()
        {
            int n = 10;
            ProcedureCaller p = new ProcedureCaller(connection);
            var args = new Dictionary<String, Object>();
            var tm = new SortedDictionary<String, Object>();
            args["X"] = tm;
            for (int i = 0; i < n; i++)
            {
                var h = new SortedDictionary<String, Object>();
                for (int j = 0; j < n; j++)
                {
                    h["" + j + "," + i] = "v" + j + "," + i;
                }
                tm["a" + i] = h;
            }
            Dictionary<String, Object> res = p.call("p1.pindex_tab2", args);
            var tm2 = (SortedDictionary<String, Object>)res["X"];
            Assert.True(tm2.Count == tm.Count);
            foreach (String k in tm.Keys)
            {

                var h1 = (SortedDictionary<String, Object>)tm[k];
                var h2 = (SortedDictionary<String, Object>)tm2[k];

                Assert.True(h1.Count == h2.Count);
                foreach (String kk in h1.Keys)
                {
                    Assert.True(h2[kk].Equals(h1[kk]));
                }
            }
        }

        [Test]
        public void testRaw()
        {
            ProcedureCaller p = new ProcedureCaller(connection);
            var args = new Dictionary<String, Object>();
            byte[] b = new byte[] { 1, 2, 3, 4, 76, 97 };
            args["X"] = b;
            args["SIZEE"] = b.Length;
            var res = p.call("p1.praw", args);
            byte[] b2 = (byte[])res["Y"];
            Assert.True(b2.Length == b.Length);
            for (int i = 0; i < b2.Length; i++)
            {
                Assert.True(b[i] == b2[i]);
            }
        }


        [Test]
        public void testRaw2()
        {
            ProcedureCaller p = new ProcedureCaller(connection);
            var args = new Dictionary<String, Object>();
            byte[] b = new byte[0];
            args["X"] = b;
            args["SIZEE"] = b.Length;
            var res = p.call("p1.praw", args);
            byte[] b2 = (byte[])res["Y"];
            Assert.True(b2 == null);
            //
            args["X"] = null;
            args["SIZEE"] = null;
            var res2 = p.call("p1.praw", args);
            byte[] b22 = (byte[])res["Y"];
            Assert.True(b22 == null);
        }

        [Test]
        public void testRawBig()
        {
            int n = 32767;
            ProcedureCaller p = new ProcedureCaller(connection);
            var args = new Dictionary<String, Object>();
            byte[] b = new byte[n];
            for (int i = 0; i < b.Length; i++)
            {
                b[i] = (byte)(i * 7 & 127);
            }
            args["X"] = b;
            args["SIZEE"] = b.Length;
            var res = p.call("p1.praw", args);
            byte[] b2 = (byte[])res["Y"];
            Assert.AreEqual(b2.Length, b.Length);
            for (int i = 0; i < b2.Length; i++)
            {
                Assert.True(b[i] == b2[i]);
            }
        }

        [Test]
        public void testVarcharBig()
        {
            int n = 32767;
            ProcedureCaller p = new ProcedureCaller(connection);
            var args = new Dictionary<String, Object>();
            char[] b = new char[n];
            for (int i = 0; i < b.Length; i++)
            {
                b[i] = (char)(i * 7 + 1 & 127);
                if (b[i] == 0)
                {
                    b[i] = (char)1;
                }
            }
            String s = new String(b);
            args["X"] = s;
            args["SIZEE"] = s.Length;
            var res = p.call("p1.pvarchar2", args);
            var s2 = (string)res["Y"];
            Assert.AreEqual(s.Length, s2.Length);
            for (int i = 0; i < s2.Length; i++)
            {
                Assert.True(s[i] == s2[i]);
            }
        }
        [Test]
        public void testVarcharout()
        {
            ProcedureCaller p = new ProcedureCaller(connection);
            var box = new Box<object>();
            p.callPositional("p1.varchar2_out", new Object[] { box });
            // on the oci style:
            // we get "" instead of null! Thr eturn value is  chr(0)||'roland'
            // and the oci code cuts this off c strings?
            // we are using managed, so everything is correct
            Assert.AreEqual("\0roland", (string)box.value);
        }
    }
}
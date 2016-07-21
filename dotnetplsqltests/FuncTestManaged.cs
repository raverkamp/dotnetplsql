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
            var res = new ProcedureCaller(connection).Call("P1.P", ar);
            System.Console.WriteLine(res);
            Assert.True(res["XO"].Equals(new Decimal(13)) && res["YO"].Equals("xx"));
        }

        [Test]
        public void t1p()
        {

            ProcedureCaller p = new ProcedureCaller(connection);
            var xo = new Box();
            var yo = new Box();
            var zo = new Box();

            p.CallPositional("P1.P", 12, "x", DateTime.Now, xo, yo, zo);
            Assert.AreEqual(xo.Value, new decimal(13));
            Assert.AreEqual(yo.Value, "xx");
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
            Dictionary<String, Object> res = new ProcedureCaller(connection).Call("P1.P2", ar);
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
            Box b = new Box();
            p.CallPositional("P1.P2", a, b);
            Dictionary<String, Object> m = (Dictionary<String, Object>)b.Value;
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
            Dictionary<String, Object> res = new ProcedureCaller(connection).Call("P1.P3", ar);
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


        public void test3WithTableBase(int size)
        {
            Dictionary<String, Object> ar = new Dictionary<String, Object>();
            List<Dictionary<String, Object>> l = new List<Dictionary<String, Object>>();
            DataTable t = new DataTable();
            t.Columns.Add(new DataColumn("X", typeof(decimal)));
            t.Columns.Add(new DataColumn("Y", typeof(string)));
            t.Columns.Add(new DataColumn("Z", typeof(DateTime)));

            for (int i = 0; i < size; i++)
            {
                var a = t.NewRow();
                a["X"] = new Decimal(i);
                a["Y"] = "x" + i;
                a["Z"] = DateTime.Now;
                t.Rows.Add(a);
            }
            ar["A"] = t;
            Dictionary<String, Object> res = new ProcedureCaller(connection).Call("P1.P3", ar);
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
            Dictionary<String, Object> res = new ProcedureCaller(connection).Call("P1.P4", ar);
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
            var res = new ProcedureCaller(connection).Call("P1.P2", ar);
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
            Box b = new Box();
            new ProcedureCaller(connection).CallPositional("P1.P2", a, b);
            var m = (Dictionary<String, Object>)b.Value;
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
            var res = new ProcedureCaller(connection).Call("P1.P4", ar);
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
            var m = new ProcedureCaller(connection).Call("proc1", a);
        }

        [Test]
        public void test8()
        {
            var a = new Dictionary<String, Object>();
            var m = new ProcedureCaller(connection).Call("p1.p6", a);
        }

        [Test]
        public void test9()
        {
            var a = new Dictionary<String, Object>();
            a["A"] = -123;
            a["B"] = "rote gruetze";
            var dat = new DateTime(2014, 12, 3, 23, 45, 1, 0);
            a["C"] = dat;
            var m = new ProcedureCaller(connection).Call("p1.f7", a);
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
                var m = new ProcedureCaller(connection).Call("p1.p8", a);
                var y = (bool)(m["Y"]);
                Assert.True(!y);
            }
            {
                a["X"] = false;
                var m = new ProcedureCaller(connection).Call("p1.p8", a);
                var y = (bool)(m["Y"]);
                Assert.True(y);
            }
            {
                a["X"] = null;
                var m = new ProcedureCaller(connection).Call("p1.p8", a);
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
                var m = new ProcedureCaller(connection).Call("p1.p9", a);
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
        //    Box b = new Box();
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
            res = p.Call("p1.no_args", a);
            res = p.Call("\"P1\".no_args", a);
            res = p.Call("\"P1\".\"NO_ARGS\"", a);
            res = p.Call("p1.\"NO_ARGS\"", a);
            res = p.Call("p1.\"NO_ARGS\"", a);
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
                res = p.Call("p1.this_proc_does_not_exist", a);
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
        public void TestSysRefCursorAsDataTable()
        {
            ProcedureCaller p = new ProcedureCaller(connection);

            Box b = new Box();
            var dat = new DateTime(2001, 12, 1);
            var bytes = new byte[] { 1, 0, 55, 4, 5 };
            p.CallPositional("p1.pcursor1", 17, "xyz", dat, bytes, b);
            var tab = (System.Data.DataTable)b.Value;
            Assert.AreEqual(tab.Rows.Count, 2);
            var r2 = tab.Rows[1];
            Assert.AreEqual(r2["A"], "xyz");
            Assert.AreEqual(r2["B"], new decimal(17));
            Assert.AreEqual(r2["C"], dat);
            Assert.AreEqual(r2["D"], bytes);
        }



        [Test]
        public void TestRefCursorAsDataTable()
        {
            ProcedureCaller p = new ProcedureCaller(connection);
            Box b = new Box();
            DateTime dat = new DateTime(2001, 12, 1);
            var bytes = new byte[] { 1, 0, 55, 4, 5 };
            p.CallPositional("p1.pcursor2", 17, "xyz", dat, bytes, b);
            var tab = (DataTable)b.Value;
            Assert.AreEqual(tab.Rows.Count, 2);
            var r2 = tab.Rows[1];
            Assert.AreEqual(r2["V"], "xyz");
            Assert.AreEqual(r2["N"], new Decimal(17));
            Assert.AreEqual(r2["D"], dat);
            Assert.AreEqual(r2["R"], bytes);


        }

        [Test]
        public void TestRefCursor3()
        {
            ProcedureCaller p = new ProcedureCaller(connection);
            Box b = new Box();
            Exception ex = null;
            try
            {
                p.CallPositional("p1.pcursor3", b);
            }
            catch (Exception exe)
            {
                ex = exe;
            }
            Assert.True(ex != null && ex is ApplicationException);
            Assert.True(ex.Message.Contains("%rowtype"));
        }

        [Test]
        public void TestRefCursor4()
        {
            var p = new ProcedureCaller(connection);
            var b = new Box();
            p.CallPositional("p1.exec_query", "select to_number(null) as a,'' as b, to_date(null) as c, hextoraw('') as d from dual", b);
            var t = (DataTable)b.Value;
            Assert.AreEqual(1, t.Rows.Count);
            Assert.True(t.Rows[0].IsNull(0) && t.Rows[0].IsNull(1) && t.Rows[0].IsNull(2) && t.Rows[0].IsNull(3));
        }

        [Test]
        public void TestRefCursor5()
        {
            var p = new ProcedureCaller(connection);
            var b = new Box();
            p.CallPositional("p1.exec_query", "select hextoraw('0a0b00ff') as a from dual", b);
            var t = (DataTable)b.Value;
            Assert.AreEqual(1, t.Rows.Count);
            var bytes = (byte[])t.Rows[0][0];
            Assert.AreEqual(bytes, new byte[] { 10, 11, 0, 255 });
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
            var res = p.Call("p1.pindex_tab", args);
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
            Dictionary<String, Object> res = p.Call("p1.pindex_tab2", args);
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
            var res = p.Call("p1.praw", args);
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
            var res = p.Call("p1.praw", args);
            byte[] b2 = (byte[])res["Y"];
            Assert.True(b2 == null);
            //
            args["X"] = null;
            args["SIZEE"] = null;
            var res2 = p.Call("p1.praw", args);
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
            var res = p.Call("p1.praw", args);
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
            var res = p.Call("p1.pvarchar2", args);
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
            var box = new Box();
            p.CallPositional("p1.varchar2_out", new Object[] { box });
            // on the oci style:
            // we get "" instead of null! Thr eturn value is  chr(0)||'roland'
            // and the oci code cuts this off c strings?
            // we are using managed, so everything is correct
            Assert.AreEqual("\0roland", (string)box.Value);
        }
        
        static bool IsNumericType(object o)
        {
            switch (Type.GetTypeCode(o.GetType()))
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    return true;
                default:
                    return false;
            }
        }
        
        static bool same(object o1, object o2)
        {
            if (o1 == null || o1 == DBNull.Value)
            {
                if (o2 == null || o2 == DBNull.Value)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            if (o2 == null || o2 == DBNull.Value)
            {
                return false;
            }
            if (o1.Equals(o2))
            {
                return true;
            }


            if (IsNumericType(o1) && IsNumericType(o2))
            {
                var n1 = Convert.ToDecimal(o1);
                var n2 = Convert.ToDecimal(o2);
                return n1.Equals(n2);
            }
            if (o1 is byte[] && o2 is byte[])
            {
                var b1 = (byte[])o1;
                var b2 = (byte[])o2;
                if (b1.Length != b2.Length)
                {
                    return false;
                }
                for (int i = 0; i < b1.Length; i++)
                {
                    if (b1[i] != b2[i])
                    {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        static void AssertSame(DataRow r, Dictionary<String, Object> d)
        {
            var t = r.Table;
            Assert.AreEqual(t.Columns.Count, d.Count);
            foreach (String s in d.Keys)
            {
                Assert.True(same(r[s], d[s]));
            }
        }

        static void AssertSame(DataTable t, List<Object> l)
        {
            if (t == null && l == null)
            {
                return;
            }
            Assert.AreEqual(l.Count, t.Rows.Count);
            for (int i = 0; i < l.Count; i++)
            {
                AssertSame(t.Rows[i], (Dictionary<String, Object>)l[i]);
            }
        }
        
        [Test]
        public void Bigrectest()
        {
            // type bigrec is record (num number,int integer,v1 varchar2(2000 char),
            //                        b boolean, d date, r raw(2000));

            ProcedureCaller p = new ProcedureCaller(connection);
            var tab = new DataTable();
            tab.Columns.Add(new DataColumn("NUM", typeof(decimal)));
            tab.Columns.Add(new DataColumn("INT", typeof(Int32)));
            tab.Columns.Add(new DataColumn("V1", typeof(String)));
            tab.Columns.Add(new DataColumn("B", typeof(bool)));
            tab.Columns.Add(new DataColumn("D", typeof(DateTime)));
            tab.Columns.Add(new DataColumn("R", typeof(byte[])));
            var b = new Box();

            p.CallPositional("p1.bigrectest", null, b);
            Assert.IsNull(b.Value);

            p.CallPositional("p1.bigrectest", tab, b);
            var l = (List<Object>)b.Value;
            AssertSame(tab, l);

            tab.Rows.Add(tab.NewRow());
            p.CallPositional("p1.bigrectest", tab, b);
            l = (List<Object>)b.Value;
            l.Reverse();
            AssertSame(tab, l);
            var date = DateTime.Now;
            date = new DateTime(date.Year, date.Month, date.Day, date.Hour, date.Minute, date.Second, date.Kind);
            tab.Rows.Add(1, 1, "a", true, date, new byte[] { 1 });
            p.CallPositional("p1.bigrectest", tab, b);
            l = (List<Object>)b.Value;
            l.Reverse();
            AssertSame(tab, l);

            date = new DateTime(2013, 3, 4, 4, 5, 1);
            tab.Rows.Add(1.126356, -1, "a", null, date, new byte[2000] );
            p.CallPositional("p1.bigrectest", tab, b);
            l = (List<Object>)b.Value;
            l.Reverse();
            AssertSame(tab, l);

            for (int i = 0; i < 1000; i++)
            {
                var by = new byte[i + 31000];
                for (int j = 0; j < by.Length; j++)
                {
                    by[j] = (byte)(i * 7 & 255);
                }
                var d = new decimal(i * 2000 + Math.Sin(i));
                tab.Rows.Add(d, i*20001, "a", false, date, by);
            }
            p.CallPositional("p1.bigrectest", tab, b);
            l = (List<Object>)b.Value;
            l.Reverse();
            AssertSame(tab, l);
        }
    }
}
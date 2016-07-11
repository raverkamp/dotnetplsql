
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Oracle.DataAccess.Client;
using System.Data;
using Oracle.DataAccess.Types;
namespace spinat.dotnetplsql
{


    public class ConversionException : Exception
    {

        public ConversionException(String bla)
        {

        }
    }

    public class Box<X>
    {

        public X value;

        public Box()
        {
            this.value = default(X);
        }

        public Box(X x)
        {
            this.value = x;
        }
    }

    public class AtomicInteger
    {
        int x = 0;
        public int incrementAndGet()
        {
            x++;
            return x;
        }
    }

    /*
    DBMS_UTILITY.NAME_RESOLVE (
    name          IN  VARCHAR2, 
    context       IN  NUMBER,
    schema        OUT VARCHAR2, 
    part1         OUT VARCHAR2, 
    part2         OUT VARCHAR2,
    dblink        OUT VARCHAR2, 
    part1_type    OUT NUMBER, 
    object_number OUT NUMBER);
    
    5 - synonym
    7 - procedure (top level)
    8 - function (top level)
    9 - package
    */
    class ResolvedName
    {

        public readonly String schema;
        public readonly String part1;
        public readonly String part2;
        public readonly String dblink;
        public readonly int part1_type;
        public readonly String object_number;

        public String toString()
        {
            return "" + schema + ", " + part1 + ", " + part2 + ", " + dblink + ", " + part1_type + ", " + object_number;
        }

        public ResolvedName(String schema,
                String part1,
                String part2,
                String dblink,
                int part1_type,
                String object_number)
        {
            this.schema = schema;
            this.part1 = part1;
            this.part2 = part2;
            this.dblink = dblink;
            this.part1_type = part1_type;
            this.object_number = object_number;
        }
    }

    class ArgArrays
    {

        public List<decimal?> decimall = new List<decimal?>();
        public List<String> varchar2 = new List<String>();
        public List<DateTime?> date = new List<DateTime?>();
        public List<byte[]> raw = new List<byte[]>();

        public void addNumber(Object n)
        {
            if (n == null)
            {
                this.decimall.Add(null);
            }
            else if (n is int)
            {
                this.decimall.Add(new decimal((int)n));
            }
            else if (n is long)
            {
                this.decimall.Add(new decimal((long)n));
            }
            else if (n is System.Numerics.BigInteger)
            {
                this.decimall.Add((decimal)((System.Numerics.BigInteger)n));
            }
            else if (n is decimal)
            {
                this.decimall.Add((decimal)n);
            }
            else if (n is Double)
            {
                this.decimall.Add(new decimal((Double)n));
            }
            else
            {
                throw new ApplicationException("unsupported number type");
            }
        }

        public void addString(String s)
        {
            this.varchar2.Add(s);
        }

        public void addDateTime(DateTime? d)
        {
            if (!d.HasValue)
            {
                this.date.Add(null);
            }
            else
            {
                this.date.Add(d);
            }
        }

        public void addRaw(byte[] r)
        {
            this.raw.Add(r);
        }
    }

    class ResArrays
    {

        public List<decimal?> decimall = new List<decimal?>();
        public List<String> varchar2 = new List<String>();
        public List<DateTime?> date = new List<DateTime?>();
        public List<byte[]> raw = new List<byte[]>();

        public int posd = 0;
        public int posv = 0;
        public int posDateTime = 0;
        public int posr = 0;

        public String readString()
        {
            String res = varchar2[posv];
            this.posv++;
            return res;
        }

        public decimal? readDecimal()
        {
            decimal? res = decimall[posd];
            this.posd++;
            return res;
        }

        public DateTime? readDateTime()
        {
            DateTime? ts = this.date[posDateTime];
            posDateTime++;
            return ts;
        }

        public byte[] readRaw()
        {
            byte[] res = raw[posr];
            posr++;
            return res;
        }
    }


    // represents types from PL/SQL
    abstract class Type
    {

        public abstract String plsqlName();

        // the data is transfered to the database in three tables one for numbers
        // one for strings and one for DateTimes. The ArgArrays contains this data.
        // this method copies the data for Object o into ArgArrays a based on
        // the type
        public abstract void fillArgArrays(ArgArrays a, Object o);

        // this is the inverse process . The result data is returned from 
        // the database in three tables. Reconstruct the objects from the 
        // data in the arrays based on the type
        public abstract Object readFromResArrays(ResArrays a);

        // generate the PL/SQL code to read the data for the arguments from the 
        // three PL/SQL arrays
        // when reading out tables we need index variables, to keep the index variables
        // distinct each gets its own number, we track that with AtomicInteger
        // there is no concurrency involved we just need a box with an integer
        // the generated pl/SQL block should only depend should depend deterministically
        // on the procedure arguments. We do not want to blow up the statement cache
        public abstract void genReadOutThing(System.Text.StringBuilder sb, AtomicInteger counter, String target);

        // generate the PL/SQL code to write the data from the OUT and IN/OUT 
        // and the return value to the three arrays
        // the reason for the AtomicInteger is the same as above
        public abstract void genWriteThing(System.Text.StringBuilder sb, AtomicInteger counter, String source);

    }

    // the PL/SQL standrad types, identfied by their name DateTime, NUMBER
    class NamedType : Type
    {

        public String name; // number, integer ...


        public override String plsqlName()
        {
            if (this.name.Equals("PL/SQL BOOLEAN"))
            {
                return "boolean";
            }
            else
            {
                return this.name;
            }
        }


        public override void fillArgArrays(ArgArrays a, Object o)
        {
            if (this.name.Equals("NUMBER")
                    || this.name.Equals("INTEGER")
                    || this.name.Equals("BINARY_INTEGER"))
            {
                a.addNumber(o);
            }
            else if (this.name.Equals("PL/SQL BOOLEAN"))
            {
                if (o == null)
                {
                    a.addNumber(null);
                }
                else
                {
                    bool b = (Boolean)o;
                    a.addNumber(b ? 1 : 0);
                }

            }
            else if (this.name.Equals("DATE"))
            {
                a.addDateTime((DateTime?)o);
            }
            else
            {
                throw new ApplicationException("unsupported named type");
            }
        }


        public override Object readFromResArrays(ResArrays a)
        {
            if (this.name.Equals("NUMBER")
                    || this.name.Equals("INTEGER")
                    || this.name.Equals("BINARY_INTEGER"))
            {
                return a.readDecimal();
            }
            else if (this.name.Equals("DATE"))
            {
                return a.readDateTime();
            }
            else if (this.name.Equals("PL/SQL BOOLEAN"))
            {
                decimal? x = a.readDecimal();
                if (x == null)
                {
                    return null;
                }
                else
                {
                    return x.Equals(decimal.One);
                }
            }
            else
            {
                throw new ApplicationException("unsupported named type: " + this.name);
            }
        }


        public override void genWriteThing(System.Text.StringBuilder sb, AtomicInteger counter, String source)
        {
            if (this.name.Equals("NUMBER")
                    || this.name.Equals("INTEGER")
                    || this.name.Equals("BINARY_INTEGER"))
            {
                sb.Append("an.extend; an(an.last):= " + source + ";\n");
            }
            else if (this.name.Equals("DATE"))
            {
                sb.Append("ad.extend; ad(ad.last):= " + source + ";\n");
            }
            else if (this.name.Equals("PL/SQL BOOLEAN"))
            {
                sb.Append("an.extend; an(an.last):= case when " + source + " then 1 when not " + source + " then 0 else null end;\n");
            }
            else
            {
                throw new ApplicationException("unsupported base type");
            }
        }


        public override void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target)
        {
            if (this.name.Equals("NUMBER")
                    || this.name.Equals("INTEGER")
                    || this.name.Equals("BINARY_INTEGER"))
            {
                sb.Append(target).Append(":= an(inn);inn:=inn+1;\n");
            }
            else if (this.name.Equals("DATE"))
            {
                sb.Append(target).Append(":= ad(ind); ind := ind+1;\n");
            }
            else if (this.name.Equals("PL/SQL BOOLEAN"))
            {
                sb.Append(target).Append(":= an(inn)=1; inn := inn+1;\n");
            }
            else
            {
                throw new ApplicationException("unsupported base type");
            }
        }
    }

    class Varchar2Type : Type
    {

        public String name; // varchar2, number, integer ...
        public int size; // 0 if unbounded, i.e. as direct parameter


        public override String plsqlName()
        {
            if (size == 0)
            {
                return "varchar2(32767)";
            }
            else
            {
                return "varchar2(" + size + ")";
            }
        }

        public override void fillArgArrays(ArgArrays a, Object o)
        {
            String s = (String)o;
            if (s == null)
            {
                a.addString(s);
            }
            else
            {
                int allowed_size = this.size == 0 ? 32767 : this.size;
                if (s.Length <= allowed_size)
                {
                    a.addString(s);
                }
                else
                {
                    throw new ConversionException("string is to large, allowed are "
                            + allowed_size + ", given length " + s.Length);
                }
            }
        }


        public override Object readFromResArrays(ResArrays a)
        {
            return a.readString();
        }


        public override void genWriteThing(StringBuilder sb, AtomicInteger counter, String source)
        {
            sb.Append("av.extend; av(av.last) := " + source + ";\n");
        }


        public override void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target)
        {
            sb.Append(target).Append(":= av(inv); inv := inv+1;\n");
        }
    }

    class RawType : Type
    {

        public String name; // varchar2, number, integer ...
        public int size; // 0 if unbounded, i.e. as direct parameter


        public override String plsqlName()
        {
            if (size == 0)
            {
                return "raw(32767)";
            }
            else
            {
                return "raw(" + size + ")";
            }
        }


        public override void fillArgArrays(ArgArrays a, Object o)
        {
            byte[] b = (byte[])o;
            if (b == null)
            {
                a.addRaw(b);
            }
            else
            {
                int allowed_size = this.size == 0 ? 32767 : this.size;
                if (b.Length <= allowed_size)
                {
                    a.addRaw(b);
                }
                else
                {
                    throw new ConversionException("raw/byte[] is to large, allowed are "
                            + allowed_size + ", given length " + b.Length);
                }
            }
        }


        public override Object readFromResArrays(ResArrays a)
        {
            return a.readRaw();
        }


        public override void genWriteThing(StringBuilder sb, AtomicInteger counter, String source)
        {
            sb.Append("ar.extend; ar(ar.last) := " + source + ";\n");
        }


        public override void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target)
        {
            sb.Append(target).Append(":= ar(inr); inr := inr+1;\n");
        }
    }

    class Field
    {

        public String name;
        public Type type;
    }



    public class ProcedureCaller
    {


        private OracleConnection connection;
        private String numberTableName = "NUMBER_ARRAY";
        private String varchar2TableName = "VARCHAR2_ARRAY";
        private String DateTimeTableName = "DATE_ARRAY";
        private String rawTableName = "RAW_ARRAY";

        // unfortunately the JDBC retrival of Array Descriptors does not care about
        // set current_schema = 
        // therefore we resolve the name and store schema.name in these fields
        private String effectiveNumberTableName = null;
        private String effectiveVarchar2TableName = null;
        private String effectiveDateTimeTableName = null;
        private String effectiveRawTableName = null;

        public ProcedureCaller(OracleConnection connection)
        {
            this.connection = connection;
        }

        /**
         * @return the numberTableName
         */
        public String getNumberTableName()
        {
            return numberTableName;
        }

        /**
         * @param numberTableName the numberTableName to set
         */
        public void setNumberTableName(String numberTableName)
        {
            this.numberTableName = numberTableName;
            this.effectiveNumberTableName = null;
        }

        /**
         * @return the varchar2TableName
         */
        public String getVarchar2TableName()
        {
            return varchar2TableName;
        }

        /**
         * @param varchar2TableName the varchar2TableName to set
         */
        public void setVarchar2TableName(String varchar2TableName)
        {
            this.varchar2TableName = varchar2TableName;
            this.effectiveVarchar2TableName = null;
        }

        /**
         * @return the DateTimeTableName
         */
        public String getDateTimeTableName()
        {
            return DateTimeTableName;
        }

        /**
         * @param DateTimeTableName the DateTimeTableName to set
         */
        public void setDateTimeTableName(String DateTimeTableName)
        {
            this.DateTimeTableName = DateTimeTableName;
            this.effectiveDateTimeTableName = null;
        }

        /**
         * @return the DateTimeTableName
         */
        public String getRawTableName()
        {
            return rawTableName;
        }

        /**
         * @param DateTimeTableName the DateTimeTableName to set
         */
        public void setRawTableName(String DateTimeTableName)
        {
            this.rawTableName = DateTimeTableName;
            this.effectiveRawTableName = null;
        }

        private static String getStringParam(OracleCommand c, String name)
        {
            OracleParameter p = c.Parameters[name];
            if (Convert.IsDBNull(p.Value))
            {
                return null;
            }
            if (p.Value is string)
            {
                return (String)p.Value;
            }

            if (p.Value is Oracle.DataAccess.Types.OracleString)
            {
                var x = (Oracle.DataAccess.Types.OracleString)p.Value;
                if (x.IsNull)
                {
                    return null;
                }
                else
                {
                    return x.Value;
                }
            }
            throw new ApplicationException("failed paramter retrieve");
        }


        private static ResolvedName resolveName(OracleConnection con, String name, bool typeContext)
        {
            using (OracleCommand cstm = con.CreateCommand())
            {
                cstm.CommandText = "begin dbms_utility.name_resolve(:p1,:p2,:p3,:p4,:p5,:p6,:p7,:p8);end;";
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P1";
                    p.DbType = DbType.String;
                    p.Value = name;
                    cstm.Parameters.Add(p);
                }

                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P2";
                    p.OracleDbType = OracleDbType.Int32;
                    if (typeContext)
                    {
                        p.Value = 7;
                    }
                    else
                    {
                        p.Value = 1; // is context PL/SQL code
                    }
                    cstm.Parameters.Add(p);
                }
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P3";
                    p.OracleDbType = OracleDbType.Varchar2;
                    p.Direction = ParameterDirection.Output;
                    p.Size = 1000;
                    cstm.Parameters.Add(p);
                }
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P4";
                    p.OracleDbType = OracleDbType.Varchar2;
                    p.Direction = ParameterDirection.Output;
                    p.Size = 1000;
                    cstm.Parameters.Add(p);
                }
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P5";
                    p.OracleDbType = OracleDbType.Varchar2;
                    p.Direction = ParameterDirection.Output;
                    p.Size = 1000;
                    cstm.Parameters.Add(p);
                }
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P6";
                    p.OracleDbType = OracleDbType.Varchar2;
                    p.Direction = ParameterDirection.Output;
                    p.Size = 1000;
                    cstm.Parameters.Add(p);
                }
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P7";
                    p.OracleDbType = OracleDbType.Int32;
                    p.Direction = ParameterDirection.Output;
                    cstm.Parameters.Add(p);
                }
                {
                    var p = cstm.CreateParameter();
                    p.ParameterName = "P8";
                    p.OracleDbType = OracleDbType.Varchar2;
                    p.Size = 1000;
                    p.Direction = ParameterDirection.Output;
                    cstm.Parameters.Add(p);
                }
                cstm.ExecuteNonQuery();
                String schema = getStringParam(cstm, "P3");
                String part1 = getStringParam(cstm, "P4");
                String part2 = getStringParam(cstm, "P5");
                String dblink = getStringParam(cstm, "P6");
                int part1_type;
                if (cstm.Parameters["P7"].Value == null)
                {
                    part1_type = 0;
                }
                else
                {
                    var x = (Oracle.DataAccess.Types.OracleDecimal)cstm.Parameters["P7"].Value;
                    if (x.IsNull)
                    {
                        part1_type = 0;
                    }
                    else
                    {
                        part1_type = (int)x.Value;
                    }
                }
                String object_number = getStringParam(cstm, "P8");
                return new ResolvedName(schema, part1, part2, dblink, part1_type, object_number);
            }
        }

        private String computeEffectiveName(String name)
        {
            if (name.Contains("."))
            {
                return name;
            }
            ResolvedName rn = resolveName(connection, name, true);
            return rn.schema + "." + name;
        }

        // determine the type of an index by table
        // it is not possible to do this by looking at the data dictionary
        // so we execute a small block to determine this
        // returns V for index by varchar2 and I for index by binary_integer
        // this is a dirty trick 
        private char isIndexByVarcharOrInt(String owner, String package_, String type)
        {
            using (OracleCommand s = this.connection.CreateCommand())
            {
                s.CommandText = "declare a " + owner + "." + package_ + "." + type + ";\n"
                + " x varchar2(1):='y';\n"
                + "begin\n"
                + "begin\n"
                + "if a.exists('akl') then\n"
                    // there has to something here otherwise oracle will optimize
                    // to much, just the statement null; will not work!
                + "  x:='z';\n"
                + "end if;\n"
                + "x:='X';\n"
                + "exception when others then\n"
                + " x:= null;\n"
                + " end;\n"
                + ":p1:=x;\n"
                    + "end;";
                OracleParameter p = s.CreateParameter();
                p.ParameterName = "P1";
                p.Direction = ParameterDirection.Output;
                p.OracleDbType = OracleDbType.Varchar2;
                p.Size = 10;
                s.Parameters.Add(p);
                s.ExecuteNonQuery();
                var x = (Oracle.DataAccess.Types.OracleString)p.Value;
                return x.IsNull ? 'I' : 'V';
            }
        }



        class RecordType : Type
        {

            public String owner;
            public String package_;
            public String name;
            public List<Field> fields;


            public override String plsqlName()
            {
                return this.owner + "." + this.package_ + "." + this.name;
            }


            public override void fillArgArrays(ArgArrays a, Object o)
            {
                if (o is Dictionary<String, Object>)
                {
                    Dictionary<String, Object> m = (Dictionary<String, Object>)o;
                    foreach (Field f in this.fields)
                    {
                        Object x;
                        if (m.ContainsKey(f.name))
                        {
                            x = m[f.name];
                            f.type.fillArgArrays(a, x);
                        }
                        else
                        {
                            throw new ConversionException("slot not found: " + f.name);
                        }
                    }
                }
            }


            public override Object readFromResArrays(ResArrays a)
            {
                Dictionary<String, Object> m = new Dictionary<String, Object>();
                foreach (Field f in this.fields)
                {
                    Object o = f.type.readFromResArrays(a);
                    m[f.name] = o;
                }
                return m;
            }


            public override void genWriteThing(StringBuilder sb, AtomicInteger counter, String source)
            {
                foreach (Field f in this.fields)
                {
                    String a = source + "." + f.name;
                    f.type.genWriteThing(sb, counter, a);
                }
            }


            public override void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target)
            {
                foreach (Field f in this.fields)
                {
                    String a = target + "." + f.name;
                    f.type.genReadOutThing(sb, counter, a);
                }
            }
        }

        // type bla is table of blub;
        // no indexed by
        class TableType : Type
        {

            public String owner;
            public String package_;
            public String name;
            public Type slottype;

            public override String plsqlName()
            {
                return this.owner + "." + this.package_ + "." + this.name;
            }


            public override void fillArgArrays(ArgArrays a, Object o)
            {
                if (o == null)
                {
                    a.addNumber((decimal?)null);
                }
                else
                {
                    System.Collections.IList l = (System.Collections.IList)o;
                    a.addNumber(l.Count);
                    foreach (Object x in l)
                    {
                        this.slottype.fillArgArrays(a, x);
                    }
                }
            }


            public override Object readFromResArrays(ResArrays a)
            {
                decimal? b = a.readDecimal();
                if (!b.HasValue)
                {
                    return null;
                }
                else
                {
                    int size = (int)b.Value;
                    List<Object> res = new List<Object>();
                    for (int i = 0; i < size; i++)
                    {
                        res.Add(this.slottype.readFromResArrays(a));
                    }
                    return res;
                }
            }


            public override void genWriteThing(StringBuilder sb, AtomicInteger counter, String source)
            {
                sb.Append("an.extend;\n");
                sb.Append(" if " + source + " is null then\n");
                sb.Append("    an(an.last) := null;\n");
                sb.Append("else \n");
                sb.Append("  an(an.last) := nvl(" + source + ".last, 0);\n");
                String index = "i" + counter.incrementAndGet();
                sb.Append("for " + index + " in 1 .. nvl(" + source + ".last,0) loop\n");
                this.slottype.genWriteThing(sb, counter, source + "(" + index + ")");
                sb.Append("end loop;\n");
                sb.Append("end if;\n");
            }


            public override void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target)
            {
                sb.Append("size_ := an(inn); inn:= inn+1;\n");
                sb.Append("if size_ is null then\n");
                sb.Append("  ").Append(target).Append(":=null;\n");
                sb.Append("else\n");
                sb.Append(" ").Append(target).Append(" := new ")
                        .Append(this.package_).Append(".").Append(this.name).Append("();\n");
                String index = "i" + counter.incrementAndGet();
                String newTarget = target + "(" + index + ")";
                sb.Append("  for ").Append(index).Append(" in 1 .. size_ loop\n");
                sb.Append("" + target + ".extend();\n");
                this.slottype.genReadOutThing(sb, counter, newTarget);
                sb.Append("end loop;\n");
                sb.Append("end if;\n");
            }
        }

        class IndexByStringTableType : Type
        {

            public String owner;
            public String package_;
            public String name;
            public Type slottype;

            public override String plsqlName()
            {
                return this.owner + "." + this.package_ + "." + this.name;
            }


            public override void fillArgArrays(ArgArrays a, Object o)
            {
                if (o == null)
                {
                    a.addNumber((decimal?)null);
                }
                else
                {
                    SortedDictionary<String, Object> tm = (SortedDictionary<String, Object>)o;
                    a.addNumber(tm.Count);
                    foreach (String k in tm.Keys)
                    {
                        var v = tm[k];
                        a.addString(k);
                        this.slottype.fillArgArrays(a, v);
                    }
                }
            }


            public override Object readFromResArrays(ResArrays a)
            {
                decimal? b = a.readDecimal();
                if (b == null)
                {
                    return null;
                }
                else
                {
                    int size = (int)b.Value;
                    SortedDictionary<String, Object> res = new SortedDictionary<String, Object>();
                    for (int i = 0; i < size; i++)
                    {
                        String k = a.readString();
                        res[k] = this.slottype.readFromResArrays(a);
                    }
                    return res;
                }
            }


            public override void genWriteThing(StringBuilder sb, AtomicInteger counter, String source)
            {
                sb.Append("  an.extend;\n");
                sb.Append("  an(an.last) := " + source + ".count;\n");
                String index = "i" + counter.incrementAndGet();
                sb.Append("declare " + index + " varchar2(32000) := " + source + ".first;\n");
                sb.Append("begin\n");
                sb.Append(" loop\n");
                sb.Append("exit when " + index + " is null;\n");
                sb.Append("  av.extend; av(av.last) := " + index + ";\n");
                this.slottype.genWriteThing(sb, counter, source + "(" + index + ")");
                sb.Append(" " + index + " := " + source + ".next(" + index + ");\n");
                sb.Append("end loop;\n");
                sb.Append("end;\n");
            }


            public override void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target)
            {
                sb.Append("size_ := an(inn); inn:= inn+1;\n");
                sb.Append("if size_ is null then\n");
                sb.Append(" null;\n");
                sb.Append("else\n");
                String index = "i" + counter.incrementAndGet();
                String key = "k" + counter.incrementAndGet();
                String newTarget = target + "(" + key + ")";
                sb.Append("declare " + key + " varchar2(32000);\n");
                sb.Append("begin\n");
                sb.Append("  for ").Append(index).Append(" in 1 .. size_ loop\n");
                sb.Append("  " + key + " :=av(inv); inv := inv+1;\n");
                this.slottype.genReadOutThing(sb, counter, newTarget);
                sb.Append("end loop;\n");
                sb.Append("end;\n");
                sb.Append("end if;\n");
            }
        }

        class IndexByIntegerTableType : Type
        {

            public String owner;
            public String package_;
            public String name;
            public Type slottype;


            public override String plsqlName()
            {
                return this.owner + "." + this.package_ + "." + this.name;
            }

            public override void fillArgArrays(ArgArrays a, Object o)
            {
                if (o == null)
                {
                    a.addNumber((decimal?)null);
                }
                else
                {
                    SortedDictionary<Int32, Object> tm = (SortedDictionary<Int32, Object>)o;
                    a.addNumber(tm.Count);
                    foreach (Int32 k in tm.Keys)
                    {
                        Object v = tm[k];
                        a.addNumber(k);
                        this.slottype.fillArgArrays(a, v);
                    }
                }
            }


            public override Object readFromResArrays(ResArrays a)
            {
                decimal? b = a.readDecimal();
                if (b == null)
                {
                    return null;
                }
                else
                {
                    int size = (int)b.Value;
                    SortedDictionary<Int32, Object> res = new SortedDictionary<Int32, Object>();
                    for (int i = 0; i < size; i++)
                    {
                        int k = (int)a.readDecimal();
                        res[k] = this.slottype.readFromResArrays(a);
                    }
                    return res;
                }
            }


            public override void genWriteThing(StringBuilder sb, AtomicInteger counter, String source)
            {
                sb.Append("  an.extend;\n");
                sb.Append("  an(an.last) := " + source + ".count;\n");

                String index = "i" + counter.incrementAndGet();
                sb.Append("declare " + index + " integer := " + source + ".first;\n");
                sb.Append("begin\n");
                sb.Append(" loop\n");
                sb.Append("exit when " + index + " is null;\n");
                sb.Append("  an.extend; an(an.last) := " + index + ";\n");
                this.slottype.genWriteThing(sb, counter, source + "(" + index + ")");
                sb.Append(" " + index + " := " + source + ".next(" + index + ");\n");
                sb.Append("end loop;\n");
                sb.Append("end;\n");
            }


            public override void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target)
            {
                sb.Append("size_ := an(inn); inn:= inn+1;\n");
                sb.Append("if size_ is null then\n");
                sb.Append(" null;\n");
                sb.Append("else\n");
                String index = "i" + counter.incrementAndGet();
                String key = "k" + counter.incrementAndGet();
                String newTarget = target + "(" + key + ")";
                sb.Append("declare " + key + " integer;\n");
                sb.Append("begin\n");
                sb.Append("  for ").Append(index).Append(" in 1 .. size_ loop\n");
                sb.Append("  " + key + " :=an(inn); inn := inn+1;\n");
                this.slottype.genReadOutThing(sb, counter, newTarget);
                sb.Append("end loop;\n");
                sb.Append("end;\n");
                sb.Append("end if;\n");
            }
        }

        class SysRefCursorType : Type
        {

            // tricky : unlike for tables we do not know the size in advance
            // and we do not know the columns
            // thus when retrieving write the columns (name,type)
            //   and then write the rows
            // for a row write 1 into number and then the row data
            // if there is no more data then write 0 into number
            // support for clobs, cursor in result set?
            // maybe a procedure whcih reads out the data and returns 
            // a handle and an array (or just a string?) with the columns types for readput

            public override String plsqlName()
            {
                return "sys_refcursor";
            }


            public override void fillArgArrays(ArgArrays a, Object o)
            {
                throw new ApplicationException("sys_refcursor may not be an \"IN\" or \"IN OUT\" parameter");
            }


            public override Object readFromResArrays(ResArrays a)
            {
                int colcount = (int)a.readDecimal();
                List<String> colnames = new List<String>();
                List<String> coltypes = new List<String>();
                for (int i = 0; i < colcount; i++)
                {
                    colnames.Add(a.readString());
                    coltypes.Add(a.readString());
                }
                List<Dictionary<String, Object>> l = new List<Dictionary<String, Object>>();
                while (true)
                {
                    if ((int)a.readDecimal() == 0)
                    {
                        break;
                    }
                    Dictionary<String, Object> m = new Dictionary<String, Object>();
                    for (int i = 0; i < colcount; i++)
                    {
                        String t = coltypes[i];
                        Object o;
                        if (t.Equals("N"))
                        {
                            o = a.readDecimal();
                        }
                        else if (t.Equals("V"))
                        {
                            o = a.readString();
                        }
                        else if (t.Equals("D"))
                        {
                            o = a.readDateTime();
                        }
                        else
                        {
                            throw new ApplicationException("unknwon column type: " + t);
                        }
                        m[colnames[i]] = o;
                    }
                    l.Add(m);
                }
                return l;
            }


            public override void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target)
            {
                throw new ApplicationException("sys_refcursor may not be an \"IN\" or \"IN OUT\" parameter");
            }


            public override void genWriteThing(StringBuilder sb, AtomicInteger counter, String source)
            {
                sb.Append("declare h integer;\n");
                sb.Append(" t varchar2(100);\n");
                sb.Append(" rec_tab   DBMS_SQL.DESC_TAB;\n");
                sb.Append(" rec       DBMS_SQL.DESC_REC;\n");
                sb.Append(" x number;\n");
                sb.Append("num number;\n");
                sb.Append("dat date;\n");
                sb.Append("varc varchar2(4000);\n");
                sb.Append(" col_cnt integer;\n");
                sb.Append("begin\n");
                sb.Append(" h := DBMS_SQL.TO_CURSOR_NUMBER (" + source + ");\n");
                sb.Append(" DBMS_SQL.DESCRIBE_COLUMNS(h, col_cnt, rec_tab);\n");
                sb.Append(" an.extend; an(an.last):= col_cnt;\n");
                sb.Append(" for i in 1 .. rec_tab.last loop\n");
                sb.Append("  rec := rec_tab(i);\n");
                sb.Append("  av.extend; av(av.last):= rec.col_name;\n");
                sb.Append("if rec.col_type = dbms_types.TYPECODE_Date then\n");
                sb.Append("        dbms_sql.define_column(h, i, dat);\n");
                sb.Append(" t:=t||'D';\n");
                sb.Append("elsif rec.col_type = dbms_types.TYPECODE_NUMBER then\n");
                sb.Append("        dbms_sql.define_column(h, i, num);\n");
                sb.Append(" t:=t||'N';\n");
                sb.Append("else\n");
                sb.Append("        dbms_sql.define_column(h, i, varc, 4000);\n");
                sb.Append(" t:=t||'V';\n");
                sb.Append("end if;");
                sb.Append("av.extend;av(av.last):=substr(t,i,1);\n");
                sb.Append(" end loop;\n");
                sb.Append(" loop\n");
                sb.Append("      x := DBMS_SQL.FETCH_ROWS(h);\n");
                sb.Append("      exit when x = 0;\n");
                sb.Append("      an.extend; an(an.last):= 1\n;");
                sb.Append("      for i in 1 .. col_cnt loop\n");
                sb.Append("        case substr(t,i,1) \n");
                sb.Append("         when 'D' then\n");
                sb.Append("          DBMS_SQL.COLUMN_VALUE(h, i, dat);\n");
                sb.Append("          ad.extend; ad(ad.last) := dat;\n");
                sb.Append("        when 'N' then\n");
                sb.Append("          DBMS_SQL.COLUMN_VALUE(h, i, num);\n");
                sb.Append("          an.extend; an(an.last) := num;\n");
                sb.Append("        when 'V' then\n");
                sb.Append("          DBMS_SQL.COLUMN_VALUE(h, i, varc);\n");
                sb.Append("          av.extend; av(av.last) := varc;\n");
                sb.Append("         else raise_application_error(-20000,'BUG: unknown internal type code: '||t);\n");
                sb.Append("         end case;\n");
                sb.Append("      end loop;\n");
                sb.Append("    end loop;\n");
                sb.Append("      an.extend; an(an.last):= 0\n;");
                sb.Append("end;");

            }
        }

        class TypedRefCursorType : Type
        {

            public String owner;
            public String package_;
            public String name;
            public RecordType rectype;

            public override String plsqlName()
            {
                return "sys_refcursor";
            }


            public override void fillArgArrays(ArgArrays a, Object o)
            {
                throw new ApplicationException("ref cursor may not be an \"IN\" or \"IN OUT\" parameter");
            }


            public override Object readFromResArrays(ResArrays a)
            {
                List<Dictionary<String, Object>> l = new List<Dictionary<String, Object>>();
                while (true)
                {
                    if ((int)a.readDecimal() == 0)
                    {
                        break;
                    }
                    Dictionary<String, Object> m = (Dictionary<String, Object>)rectype.readFromResArrays(a);
                    l.Add(m);
                }
                return l;
            }


            public override void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target)
            {
                throw new ApplicationException("ref cursor may not be an \"IN\" or \"IN OUT\" parameter");
            }


            public override void genWriteThing(StringBuilder sb, AtomicInteger counter, String source)
            {
                sb.Append("declare r " + rectype.plsqlName() + ";\n");
                sb.Append("x integer;\n");
                sb.Append("begin\n");
                sb.Append("loop\n");
                sb.Append(" fetch " + source + " into r;\n");
                sb.Append("if " + source + "%notfound then\n");
                sb.Append("  exit;\n");
                sb.Append("end if;\n");
                sb.Append("an.extend;an(an.last) := 1;\n");
                rectype.genWriteThing(sb, counter, "r");
                sb.Append("end loop;\n");
                sb.Append("an.extend;an(an.last) := 0;\n");
                sb.Append("end;\n");
            }
        }

        // the arguments to a procedure/function
        class Argument
        {

            public String name;
            public String direction;
            public Type type;
        }

        // represents one procedure/function
        class Procedure
        {

            // not null if function
            public Type returnType;
            public String original_name;
            public String owner;
            public String package_; // could be null
            public String name;
            public int overload;
            public List<Argument> arguments;
            // used to store the generated pl/sql block
            public String plsqlstatement = null;
        }

        // this class corresponds 1:1 the columns in all_arguments, some columns are lft out
        // when working with the data in all_arguments it is transformed into an List
        // of this ArgumentsRow
        class ArgumentsRow
        {

            public String owner;
            public String object_name;
            public String package_name;
            public String argument_name;
            public int position;
            public int sequence;
            public int data_level;
            public String data_type;
            public int overload;
            public String in_out;
            public String type_owner;
            public String type_name;
            public String type_subname;
            public String pls_type;
            public int data_length;
        }

        private static String fetchString(IDataReader dr, String column)
        {
            int pos = dr.GetOrdinal(column);
            if (dr.IsDBNull(pos))
            {
                return null;
            }
            else
            {
                return dr.GetString(pos);
            }
        }

        private static Queue<ArgumentsRow> fetchArgumentsRows(IDataReader rs)
        {
            Queue<ArgumentsRow> res = new Queue<ArgumentsRow>();
            while (rs.Read())
            {
                ArgumentsRow r = new ArgumentsRow();
                r.owner = rs.GetString(rs.GetOrdinal("OWNER"));
                r.object_name = rs.GetString(rs.GetOrdinal("OBJECT_NAME"));
                r.package_name = fetchString(rs, "PACKAGE_NAME");
                r.argument_name = fetchString(rs, "ARGUMENT_NAME");
                r.position = (int)rs.GetDecimal(rs.GetOrdinal("POSITION"));
                r.sequence = (int)rs.GetDecimal(rs.GetOrdinal("SEQUENCE"));
                r.data_level = (int)rs.GetDecimal(rs.GetOrdinal("DATA_LEVEL"));
                r.data_type = fetchString(rs, "DATA_TYPE");
                if (rs.IsDBNull(rs.GetOrdinal("OVERLOAD")))
                {
                    r.overload = -1;
                }
                else
                {
                    r.overload = (int)rs.GetDecimal(rs.GetOrdinal("OVERLOAD"));
                }

                r.in_out = rs.GetString(rs.GetOrdinal("IN_OUT"));
                r.type_owner = fetchString(rs, "TYPE_OWNER");
                r.type_name = fetchString(rs, "TYPE_NAME");
                r.type_subname = fetchString(rs, "TYPE_SUBNAME");

                r.pls_type = fetchString(rs, "PLS_TYPE");

                if (rs.IsDBNull(rs.GetOrdinal("DATA_LENGTH")))
                {
                    r.data_length = 0;
                }
                else
                {
                    r.data_length = (int)rs.GetDecimal(rs.GetOrdinal("DATA_LENGTH"));
                }
                res.Enqueue(r);
            }
            return res;
        }

        // get a Field from Args a and advance the internal position to the position
        // after this Field.
        // due to the recursive structure of PL/SQL types this method is recursive
        private Field eatArg(Queue<ArgumentsRow> a)
        {
            ArgumentsRow r = a.Peek();
            Field f = new Field();
            f.name = r.argument_name;
            if (r.data_type.Equals("NUMBER")
                    || r.data_type.Equals("DATE")
                    || r.data_type.Equals("INTEGER")
                    || r.data_type.Equals("PL/SQL BOOLEAN")
                    || r.data_type.Equals("BINARY_INTEGER"))
            {
                NamedType t = new NamedType();
                t.name = r.data_type;
                f.type = t;
                a.Dequeue();
                return f;
            }
            if (r.data_type.Equals("VARCHAR2"))
            {
                Varchar2Type vt = new Varchar2Type();
                vt.name = "VARCHAR2";
                vt.size = r.data_length;
                f.type = vt;
                a.Dequeue();
                return f;
            }
            if (r.data_type.Equals("RAW"))
            {
                RawType vt = new RawType();
                vt.name = "RAW";
                vt.size = r.data_length;
                f.type = vt;
                a.Dequeue();
                return f;
            }
            if (r.data_type.Equals("TABLE"))
            {
                TableType t = new TableType();
                t.owner = r.type_owner;
                t.package_ = r.type_name;
                t.name = r.type_subname;
                a.Dequeue();
                Field f2 = eatArg(a);
                t.slottype = f2.type;
                f.type = t;
                return f;
            }
            if (r.data_type.Equals("PL/SQL RECORD"))
            {
                RecordType t = new RecordType();
                t.owner = r.type_owner;
                t.package_ = r.type_name;
                t.name = r.type_subname;
                t.fields = new List<Field>();
                int level = r.data_level;
                a.Dequeue();
                while (!(a.Count == 0) && a.Peek().data_level > level)
                {
                    t.fields.Add(eatArg(a));
                }
                f.type = t;
                return f;
            }
            // as of Oracle 11.2.0.2.0 ref cursors can not be part of records or be
            // part of a table
            if (r.data_type.Equals("REF CURSOR"))
            {
                a.Dequeue();
                if (a.Count == 0 || a.Peek().data_level == 0)
                {
                    SysRefCursorType t = new SysRefCursorType();
                    f.type = t;
                    return f;
                }
                TypedRefCursorType tr = new TypedRefCursorType();
                tr.owner = r.type_owner;
                tr.package_ = r.type_name;
                tr.name = r.type_subname;
                Field f2 = eatArg(a);
                if (f2.type is RecordType)
                {
                    tr.rectype = (RecordType)f2.type;
                    f.type = tr;
                    if (tr.rectype.name == null)
                    {
                        throw new ApplicationException("anonymous record types (%rowtype) are not supported");
                    }
                    return f;
                }
                else
                {
                    throw new ApplicationException("unknown record type for cursor");
                }
            }
            if (r.data_type.Equals("PL/SQL TABLE"))
            {
                char tt = isIndexByVarcharOrInt(r.type_owner, r.type_name, r.type_subname);
                if (tt == 'V')
                {
                    IndexByStringTableType t = new IndexByStringTableType();
                    t.owner = r.type_owner;
                    t.package_ = r.type_name;
                    t.name = r.type_subname;
                    a.Dequeue();
                    Field f2 = eatArg(a);
                    t.slottype = f2.type;
                    f.type = t;
                    return f;
                }
                else if (tt == 'I')
                {
                    IndexByIntegerTableType t = new IndexByIntegerTableType();
                    t.owner = r.type_owner;
                    t.package_ = r.type_name;
                    t.name = r.type_subname;
                    a.Dequeue();
                    Field f2 = eatArg(a);
                    t.slottype = f2.type;
                    f.type = t;
                    return f;
                }
                else
                {
                    throw new ApplicationException("BUG");
                }
            }
            throw new ApplicationException("unsupported type: " + r.data_type);
        }

        private Procedure eatProc(Queue<ArgumentsRow> a)
        {
            Procedure p = new Procedure();
            ArgumentsRow r = a.Peek();
            p.package_ = r.package_name;
            p.name = r.object_name;
            p.overload = r.overload;
            p.owner = r.owner;
            p.arguments = new List<Argument>();
            if (a.Peek().data_type == null)
            {
                // this is a procedure with no arguments
                a.Dequeue();
                return p;
            }
            if (r.position == 0)
            {
                // this a function the return type is the first argument, the 
                // argument name is null
                Field f = eatArg(a);
                p.returnType = f.type;
            }
            while (a.Count > 0 && a.Peek().overload == p.overload)
            {
                String io = a.Peek().in_out;
                Field f = eatArg(a);
                Argument ar = new Argument();
                ar.direction = io;
                ar.name = f.name;
                ar.type = f.type;
                p.arguments.Add(ar);
            }
            return p;
        }


        private String createStatementString(Procedure p)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("declare\n");
            sb.Append("an " + this.numberTableName + ";\n");
            sb.Append("av " + this.varchar2TableName + " ;\n");
            sb.Append("ad " + this.DateTimeTableName + ";\n");
            sb.Append("ar " + this.rawTableName + ";\n");
            sb.Append("inn integer :=1;\n");
            sb.Append("inv integer :=1;\n");
            sb.Append("ind integer :=1;\n");
            sb.Append("inr integer :=1;\n");
            sb.Append("size_ integer;\n");
            if (p.returnType != null)
            {
                sb.Append("result$ ").Append(p.returnType.plsqlName()).Append(";\n");
            }
            for (int i = 0; i < p.arguments.Count; i++)
            {
                sb.Append("p" + i + "$ ").Append(p.arguments[i].type.plsqlName());
                sb.Append(";\n");
            }
            sb.Append("begin\n");
            sb.Append("an :=:p1;\n");
            sb.Append("av :=:p2;\n");
            sb.Append("ad :=:p3;\n");
            sb.Append("ar :=:p4;\n");
            AtomicInteger counter = new AtomicInteger();
            for (int i = 0; i < p.arguments.Count; i++)
            {
                Argument a = p.arguments[i];
                if (a.direction.Equals("OUT"))
                {
                    continue;
                }
                a.type.genReadOutThing(sb, counter, "p" + i + "$");
            }
            // at this point the parameters have been filled, clear the
            // parameter arrays
            sb.Append("an:= " + this.numberTableName + "();\n");
            sb.Append("av:= " + this.varchar2TableName + "();\n");
            sb.Append("ad:= " + this.DateTimeTableName + "();\n");
            sb.Append("ar:= " + this.rawTableName + "();\n");
            // generate teh actual procedure call
            if (p.returnType != null)
            {
                sb.Append("result$:=");
            }
            sb.Append(p.original_name + "(");
            for (int i = 0; i < p.arguments.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }
                sb.Append(" " + p.arguments[i].name + " => ");
                sb.Append("p" + i + "$");
            }
            sb.Append(");\n");
            // after the procedure call
            if (p.returnType != null)
            {
                p.returnType.genWriteThing(sb, counter, "result$");
            }
            for (int i = 0; i < p.arguments.Count; i++)
            {
                Argument a = p.arguments[i];
                if (a.direction.Equals("IN"))
                {
                    continue;
                }
                a.type.genWriteThing(sb, counter, "p" + i + "$");
            }
            sb.Append(":p5:= an;\n");
            sb.Append(":p6:= av;\n");
            sb.Append(":p7:= ad;\n");
            sb.Append(":p8:= ar;\n");
            sb.Append("end;\n");
            return sb.ToString();
        }

        private ResArrays baseCall(
                Procedure p,  ArgArrays aa) //, Dictionary<String, Object> args)
        {
            if (this.effectiveNumberTableName == null)
            {
                this.effectiveNumberTableName = computeEffectiveName(this.numberTableName);
            }
            if (this.effectiveVarchar2TableName == null)
            {
                this.effectiveVarchar2TableName = computeEffectiveName(this.varchar2TableName);
            }
            if (this.effectiveDateTimeTableName == null)
            {
                this.effectiveDateTimeTableName = computeEffectiveName(this.DateTimeTableName);
            }
            if (this.effectiveRawTableName == null)
            {
                this.effectiveRawTableName = computeEffectiveName(this.rawTableName);
            }

            decimal?[] no;
            string[] vo;
            DateTime?[] do_;
            byte[][] ro;

            using (OracleCommand cstm = this.connection.CreateCommand())
            {
                cstm.CommandText = p.plsqlstatement;
                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P1";
                    //pa.OracleDbType = OracleDbType.Object;
                    pa.OracleDbTypeEx = OracleDbType.Array;
                    pa.UdtTypeName = "ROLAND.NUMBER_ARRAY";
                    pa.Direction = ParameterDirection.Input;
                    pa.Value = aa.decimall.ToArray();
                    cstm.Parameters.Add(pa);
                }


                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P2";
                    //pa.OracleDbType = OracleDbType.Object;
                    pa.OracleDbTypeEx = OracleDbType.Array;
                    pa.UdtTypeName = "ROLAND.VARCHAR2_ARRAY";
                    pa.Direction = ParameterDirection.Input;
                    pa.Value = aa.varchar2.ToArray();
                    cstm.Parameters.Add(pa);
                }
                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P3";
                    //pa.OracleDbType = OracleDbType.Object;
                    pa.OracleDbTypeEx = OracleDbType.Array;
                    pa.UdtTypeName = "ROLAND.DATE_ARRAY";
                    pa.Direction = ParameterDirection.Input;
                    pa.Value = aa.date.ToArray();
                    cstm.Parameters.Add(pa);
                }
                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P4";
                    //pa.OracleDbType = OracleDbType.Object;
                    pa.OracleDbTypeEx = OracleDbType.Array;
                    pa.UdtTypeName = "ROLAND.RAW_ARRAY";
                    pa.Direction = ParameterDirection.Input;
                    pa.Value = aa.raw.ToArray();
                    cstm.Parameters.Add(pa);
                }
                //---------------------------------------------
                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P5";
                    pa.Direction = ParameterDirection.Output;
                    pa.OracleDbType = OracleDbType.Object;
                    pa.OracleDbTypeEx = OracleDbType.Array;
                    pa.UdtTypeName = "ROLAND.NUMBER_ARRAY";
                    cstm.Parameters.Add(pa);
                }
                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P6";
                    pa.Direction = ParameterDirection.Output;
                    pa.OracleDbType = OracleDbType.Object;
                    pa.OracleDbTypeEx = OracleDbType.Array;
                    pa.UdtTypeName = "ROLAND.VARCHAR2_ARRAY";
                    cstm.Parameters.Add(pa);
                }
                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P7";
                    pa.Direction = ParameterDirection.Output;
                    pa.OracleDbType = OracleDbType.Object;
                    pa.OracleDbTypeEx = OracleDbType.Array;
                    pa.UdtTypeName = "ROLAND.DATE_ARRAY";
                    cstm.Parameters.Add(pa);
                }
                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P8";
                    pa.Direction = ParameterDirection.Output;
                    pa.OracleDbType = OracleDbType.Object;
                    pa.OracleDbTypeEx = OracleDbType.Array;
                    pa.UdtTypeName = "ROLAND.RAW_ARRAY";
                    cstm.Parameters.Add(pa);
                }

                cstm.ExecuteNonQuery();

                no = ((SimpleNumberArray)cstm.Parameters["P5"].Value).Array;
                vo = ((SimpleStringArray)cstm.Parameters["P6"].Value).Array;
                do_ = ((SimpleDateArray)cstm.Parameters["P7"].Value).Array;
                ro = ((SimpleRawArray)cstm.Parameters["P8"].Value).Array;
            }
            ResArrays ra = new ResArrays();

            foreach (decimal? d in no)
            {
                ra.decimall.Add(d);
            }

            foreach (string s in vo)
            {
                ra.varchar2.Add(s);
            }
            foreach (DateTime? dt in do_)
            {
                ra.date.Add(dt);
            }
            foreach (byte[] b in ro)
            {
                ra.raw.Add(b);
            }
            return ra;
        }

        private Dictionary<String, Object> call(
           Procedure p, Dictionary<String, Object> args)
        {
            if (p.plsqlstatement == null)
            {
                p.plsqlstatement = createStatementString(p);
            }
            ArgArrays aa = new ArgArrays();
            foreach (Argument arg in p.arguments)
            {
                if (arg.direction.Equals("OUT"))
                {
                    continue;
                }
                if (args.ContainsKey(arg.name))
                {
                    Object o = args[arg.name];
                    arg.type.fillArgArrays(aa, o);
                }
                else
                {
                    throw new ConversionException("could not find argument " + arg.name);
                }
            }
            ResArrays ra = baseCall(p, aa);

            Dictionary<String, Object> res = new Dictionary<String, Object>();
            if (p.returnType != null)
            {
                Object o = p.returnType.readFromResArrays(ra);
                res["RETURN"] = o;
            }
            foreach (Argument arg in p.arguments)
            {
                if (arg.direction.Equals("IN"))
                {
                    continue;
                }
                Object o = arg.type.readFromResArrays(ra);
                res[arg.name] = o;
            }
            return res;
        }


        // call the procedure, for each parameter there must be an entry in Object
        private Object callPositional(Procedure p, Object[] args)
        {
            if (p.plsqlstatement == null)
            {
                p.plsqlstatement = createStatementString(p);
            }
                ArgArrays aa = new ArgArrays();
                {
                    int i = 0;
                    foreach (Argument arg in p.arguments)
                    {
                        if (!arg.direction.Equals("OUT"))
                        {
                            Object o = args[i];
                            if (o is Box<Object>)
                            {
                                o = ((Box<Object>)o).value;
                            }
                            arg.type.fillArgArrays(aa, o);
                        }
                        i++;
                    }
                }
                ResArrays ra = baseCall(p, aa);
            Object result;
            if (p.returnType != null)
            {
                result = p.returnType.readFromResArrays(ra);
            }
            else
            {
                result = null;
            }
            {
                int i = 0;
                foreach (Argument arg in p.arguments)
                {
                    if (!arg.direction.Equals("IN"))
                    {

                        if (args[i] != null && args[i] is Box<Object>)
                        {
                            Object o = arg.type.readFromResArrays(ra);
                            ((Box<Object>)args[i]).value = o;
                        }
                        else
                        {
                            throw new ApplicationException("need a box for parameter " + arg.name);
                        }
                    }
                    i++;
                }
            }
            return result;
        }

        private static String sql1 = "select OWNER,OBJECT_NAME,PACKAGE_NAME,ARGUMENT_NAME,\n"
                + "POSITION,SEQUENCE,DATA_LEVEL,DATA_TYPE,\n"
                + " OVERLOAD, IN_OUT, TYPE_OWNER, TYPE_NAME, TYPE_SUBNAME, PLS_TYPE,data_length\n"
                + " from all_arguments \n"
                + " where owner = :p1 and package_name = :p2 and object_name = :p3\n"
                + " order by owner,package_name,object_name,overload,sequence";

        private static String sql2 = "select OWNER,OBJECT_NAME,PACKAGE_NAME,ARGUMENT_NAME,"
                + "POSITION,SEQUENCE,DATA_LEVEL,DATA_TYPE,"
                + " OVERLOAD, IN_OUT, TYPE_OWNER, TYPE_NAME, TYPE_SUBNAME, PLS_TYPE,data_length\n"
                + " from all_arguments \n"
                + " where object_id = :p1 \n"
                + " order by owner,package_name,object_name,overload,sequence";

        private List<Procedure> getProcsFromDB(String name)
        {

            ResolvedName rn = resolveName(this.connection, name, false);
            if (rn.dblink != null)
            {
                throw new ApplicationException("no call over dblink");
            }
            Queue<ArgumentsRow> argument_rows;
            var pstm = this.connection.CreateCommand();

            if (rn.part1_type == 7 || rn.part1_type == 8)
            {
                // this a global procedure or function
                pstm.CommandText = sql2;
                OracleParameter p = pstm.CreateParameter();
                p.ParameterName = "P1";
                p.OracleDbType = OracleDbType.Varchar2;
                p.Value = rn.object_number;
                pstm.Parameters.Add(p);
            }
            else if (rn.part1_type == 9)
            {
                if (rn.part2 == null)
                {
                    throw new ApplicationException("only package given: " + name);
                }
                // this is procedure or function in a package
                pstm.CommandText = sql1;

                {
                    OracleParameter p = pstm.CreateParameter();
                    p.ParameterName = "P1";
                    p.OracleDbType = OracleDbType.Varchar2;
                    p.Value = rn.schema;
                    pstm.Parameters.Add(p);
                }
                {
                    OracleParameter p = pstm.CreateParameter();
                    p.ParameterName = "P2";
                    p.OracleDbType = OracleDbType.Varchar2;
                    p.Value = rn.part1;
                    pstm.Parameters.Add(p);
                }
                {
                    OracleParameter p = pstm.CreateParameter();
                    p.ParameterName = "P3";
                    p.OracleDbType = OracleDbType.Varchar2;
                    p.Value = rn.part2;
                    pstm.Parameters.Add(p);
                }
            }
            else
            {
                throw new ApplicationException("this is not a gobal procedure/function, "
                        + "nor a procedure/function in a package: " + name);
            }
            using (IDataReader rs = pstm.ExecuteReader())
            {
                argument_rows = fetchArgumentsRows(rs);
            }
            pstm.Dispose();
            if (argument_rows.Count == 0)
            {
                throw new ApplicationException("procedure in package does not exist or object is not valid: " + name);
            }
            List<Procedure> procs = new List<Procedure>();
            while (argument_rows.Count != 0)
            {
                Procedure p = eatProc(argument_rows);
                p.original_name = name;
                procs.Add(p);
            }
            return procs;
        }

        private Dictionary<String, List<Procedure>> procsMap = new Dictionary<String, List<Procedure>>();

        private List<Procedure> getProcs(String name)
        {
            List<Procedure> res = null;
            if (procsMap.TryGetValue(name, out res))
            {
                return res;
            }


            res = getProcsFromDB(name);
            procsMap[name] = res;

            return res;
        }

        public Dictionary<String, Object> call(
                String name, int overload, Dictionary<String, Object> args)
        {
            List<Procedure> procs = getProcs(name);

            if (overload > procs.Count)
            {
                throw new ApplicationException("the overload does not exist for procedure/function " + name);
            }
            if (overload <= 0)
            {
                throw new ApplicationException("overload must greater or equal 1");
            }
            return call(procs[overload - 1], args);
        }

        public Dictionary<String, Object> call(
                String name, Dictionary<String, Object> args)
        {
            List<Procedure> procs = getProcs(name);
            if (procs.Count > 1)
            {
                throw new ApplicationException("procedure/function is overloaded, supply a overload: " + name);
            }
            else
            {
                return this.call(procs[0], args);
            }
        }

        public Object callPositional(String name, params Object[] args)
        {
            List<Procedure> procs = getProcs(name);
            if (procs.Count > 1)
            {
                throw new ApplicationException("procedure/function is overloaded, supply a overload: " + name);
            }
            else
            {
                return this.callPositional(procs[0], args);
            }
        }

        public Object callPositionalO(String name, int overload, params Object[] args)
        {
            List<Procedure> procs = getProcs(name);
            if (overload > procs.Count)
            {
                throw new ApplicationException("the overload does not exist for procedure/function " + name);
            }
            if (overload <= 0)
            {
                throw new ApplicationException("overload must greater or equal 1");
            }
            return this.callPositional(procs[overload - 1], args);
        }
    }
}

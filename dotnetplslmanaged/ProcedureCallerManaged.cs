using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;


namespace spinat.dotnetplslmanaged
{
    internal class Counter
    {
        int x = 0;
        public int incrementAndGet()
        {
            x++;
            return x;
        }
    }

    // the arguments to a procedure/function
    internal class Argument
    {
        public String name;
        public String direction;
        public PLSQLType type;
    }

    // represents one procedure/function
    internal class Procedure
    {
        // not null if function
        public PLSQLType returnType;
        public String original_name;
        public String owner;
        public String package_; // could be null
        public String name;
        public int overload;
        public List<Argument> arguments;
        // used to store the generated pl/sql block
        public String plsqlstatement = null;
    }

    internal class ArgArrays
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

    internal class ResArrays
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


    public class ProcedureCaller
    {


        private OracleConnection connection;
        private String numberTableName = "NUMBER_ARRAY";
        private String varchar2TableName = "VARCHAR2_ARRAY";
        private String DateTimeTableName = "DATE_ARRAY";
        private String rawTableName = "RAW_ARRAY";

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
        }

        private Boolean returnRefCursorAsDataTable = false;

        public bool ReturnRefCursorAsDataTable
        {
            set
            {
                this.returnRefCursorAsDataTable = value;
                this.procsMap.Clear();
            }

            get
            {
                return this.returnRefCursorAsDataTable;
            }
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
                var x = (Oracle.ManagedDataAccess.Types.OracleString)p.Value;
                return x.IsNull ? 'I' : 'V';
            }
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
                vt.size = r.data_length;
                f.type = vt;
                a.Dequeue();
                return f;
            }
            if (r.data_type.Equals("RAW"))
            {
                RawType vt = new RawType();
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
                    SysRefCursorType t = new SysRefCursorType(this.returnRefCursorAsDataTable);
                    f.type = t;
                    return f;
                }
                TypedRefCursorType tr = new TypedRefCursorType(this.returnRefCursorAsDataTable);
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
            sb.Append("type number_ibt is table of number index by binary_integer;\n");
            sb.Append("type varchar2_ibt is table of varchar2(32767) index by binary_integer;\n");
            sb.Append("type date_ibt is table of date index by binary_integer;\n");
            sb.Append("type raw_ibt is table of raw(32767) index by binary_integer;\n");

            sb.Append("an " + this.numberTableName + ";\n");
            sb.Append("av " + this.varchar2TableName + " ;\n");
            sb.Append("ad " + this.DateTimeTableName + ";\n");
            sb.Append("ar " + this.rawTableName + ";\n");
            sb.Append("an_ibt number_ibt;\n");
            sb.Append("av_ibt varchar2_ibt;\n");
            sb.Append("ad_ibt date_ibt;\n");
            sb.Append("ar_ibt raw_ibt;\n");
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
            sb.Append("function getn return number is begin inn:=inn+1; return an_ibt(inn-1);end;\n");
            sb.Append("function getv return varchar2 is begin inv:=inv+1; return av_ibt(inv-1);end;\n");
            sb.Append("function getd return date is begin ind:=ind+1; return ad_ibt(ind-1);end;\n");
            sb.Append("function getr return raw is begin inr:=inr+1; return ar_ibt(inr-1);end;\n");
            sb.Append("begin\n");

            sb.Append("an_ibt :=:p1;\n");
            sb.Append("av_ibt :=:p2;\n");
            sb.Append("ad_ibt :=:p3;\n");
            sb.Append("ar_ibt :=:p4;\n");

            Counter counter = new Counter();
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
            sb.Append("open :p5 for select * from table(an);\n");
            sb.Append("open :p6 for select * from table(av);\n");
            sb.Append("open :p7 for select * from table(ad);\n");
            sb.Append("open :p8 for select * from table(ar);\n");
            sb.Append("end;\n");
            return sb.ToString();
        }

        private ResArrays baseCall(
                Procedure p, ArgArrays aa)
        {
            if (p.plsqlstatement == null)
            {
                p.plsqlstatement = createStatementString(p);
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
                    pa.OracleDbType = OracleDbType.Decimal;
                    pa.CollectionType = OracleCollectionType.PLSQLAssociativeArray;
                    //pa.UdtTypeName = "ROLAND.NUMBER_ARRAY";
                    pa.Direction = ParameterDirection.Input;
                    if (aa.decimall.Count == 0)
                    {
                        pa.Value = new decimal[] { 1 };
                    }
                    else
                    {
                        pa.Value = aa.decimall.ToArray<decimal?>();
                    }
                    cstm.Parameters.Add(pa);
                }
                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P2";
                    pa.OracleDbType = OracleDbType.Varchar2;
                    pa.CollectionType = OracleCollectionType.PLSQLAssociativeArray;
                    //pa.UdtTypeName = "ROLAND.VARCHAR2_ARRAY";
                    pa.Direction = ParameterDirection.Input;
                    if (aa.varchar2.Count == 0)
                    {
                        pa.Value = new string[] { "x" };
                    }
                    else
                    {
                        pa.Value = aa.varchar2.ToArray();
                    }
                    cstm.Parameters.Add(pa);
                }
                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P3";
                    pa.OracleDbType = OracleDbType.Date;
                    pa.CollectionType = OracleCollectionType.PLSQLAssociativeArray;
                    //pa.UdtTypeName = "ROLAND.DATE_ARRAY";
                    pa.Direction = ParameterDirection.Input;
                    if (aa.date.Count == 0)
                    {
                        pa.Value = new DateTime[] { DateTime.Now };
                    }
                    else
                    {
                        pa.Value = aa.date.ToArray<DateTime?>();
                    }
                    cstm.Parameters.Add(pa);
                }

                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P4";
                    pa.OracleDbType = OracleDbType.Raw;
                    pa.CollectionType = OracleCollectionType.PLSQLAssociativeArray;
                    //pa.UdtTypeName = "ROLAND.RAW_ARRAY";
                    pa.Direction = ParameterDirection.Input;
                    if (aa.raw.Count == 0)
                    {
                        pa.Value = new Oracle.ManagedDataAccess.Types.OracleBinary[] { new OracleBinary(new byte[] { 1 }) };
                    }
                    else
                    {
                        pa.Value = aa.raw.ToArray<byte[]>();
                    }
                    cstm.Parameters.Add(pa);
                }

                //---------------------------------------------
                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P5";
                    pa.Direction = ParameterDirection.Output;
                    pa.OracleDbType = OracleDbType.RefCursor;
                    cstm.Parameters.Add(pa);
                }
                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P6";
                    pa.Direction = ParameterDirection.Output;
                    pa.OracleDbType = OracleDbType.RefCursor;

                    cstm.Parameters.Add(pa);
                }
                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P7";
                    pa.Direction = ParameterDirection.Output;
                    pa.OracleDbType = OracleDbType.RefCursor;
                    cstm.Parameters.Add(pa);
                }
                {
                    OracleParameter pa = cstm.CreateParameter();
                    pa.ParameterName = "P8";
                    pa.Direction = ParameterDirection.Output;
                    pa.OracleDbType = OracleDbType.RefCursor;
                    cstm.Parameters.Add(pa);
                }
                //Debug.WriteLine(cstm.CommandText);
                cstm.ExecuteNonQuery();
                {
                    var nc = (Oracle.ManagedDataAccess.Types.OracleRefCursor)cstm.Parameters["P5"].Value;
                    var dr = nc.GetDataReader();
                    var l = new List<decimal?>();
                    while (dr.Read())
                    {
                        var x = dr.GetOracleDecimal(0);
                        if (x.IsNull)
                        {
                            l.Add(null);
                        }
                        else
                        {
                            l.Add(x.Value);
                        }
                    }
                    no = l.ToArray();
                }
                {
                    var nv = (Oracle.ManagedDataAccess.Types.OracleRefCursor)cstm.Parameters["P6"].Value;
                    var dr = nv.GetDataReader();
                    var l = new List<String>();
                    while (dr.Read())
                    {
                        var x = dr.GetOracleString(0);
                        if (x.IsNull)
                        {
                            l.Add(null);
                        }
                        else
                        {
                            l.Add(x.Value);
                        }
                    }
                    vo = l.ToArray();
                }
                {
                    var nd = (Oracle.ManagedDataAccess.Types.OracleRefCursor)cstm.Parameters["P7"].Value;
                    var dr = nd.GetDataReader();
                    var l = new List<DateTime?>();
                    while (dr.Read())
                    {
                        var x = dr.GetOracleDate(0);
                        if (x.IsNull)
                        {
                            l.Add(null);
                        }
                        else
                        {
                            l.Add(x.Value);
                        }
                    }
                    do_ = l.ToArray();
                }
                {
                    var nv = (Oracle.ManagedDataAccess.Types.OracleRefCursor)cstm.Parameters["P8"].Value;
                    var dr = nv.GetDataReader();
                    var l = new List<byte[]>();
                    while (dr.Read())
                    {
                        var x = dr.GetOracleBinary(0);
                        if (x.IsNull)
                        {
                            l.Add(null);
                        }
                        else
                        {
                            l.Add(x.Value);
                        }
                    }
                    ro = l.ToArray();
                }

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

        private Object callPositional(Procedure p, Object[] args)
        {
            if (args.Length != p.arguments.Count)
            {
                throw new ApplicationException("wrong number of arguments for procedure " + p.name + ", expecting " + p.arguments.Count + ", got " + args.Length);
            }
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
                        if (o is Box)
                        {
                            o = ((Box)o).Value;
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

                        if (args[i] != null && args[i] is Box)
                        {
                            Object o = arg.type.readFromResArrays(ra);
                            ((Box)args[i]).Value = o;
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

            ResolvedName rn = Resolver.ResolveName(this.connection, name, false);
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

        public Dictionary<String, Object> Call(
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

        public Dictionary<String, Object> Call(
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

        public Object CallPositional(String name, params Object[] args)
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

        public Object CallPositionalOverload(String name, int overload, params Object[] args)
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
        //}
    }
}


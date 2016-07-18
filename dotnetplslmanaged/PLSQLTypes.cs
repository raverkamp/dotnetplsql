using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace spinat.dotnetplslmanaged
{
    // represents types from PL/SQL
    abstract class PLSQLType
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
        public abstract void genReadOutThing(System.Text.StringBuilder sb, Counter counter, String target);

        // generate the PL/SQL code to write the data from the OUT and IN/OUT 
        // and the return value to the three arrays
        // the reason for the AtomicInteger is the same as above
        public abstract void genWriteThing(System.Text.StringBuilder sb, Counter counter, String source);

    }

    // the PL/SQL standrad types, identfied by their name DateTime, NUMBER
    class NamedType : PLSQLType
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


        public override void genWriteThing(System.Text.StringBuilder sb, Counter counter, String source)
        {
            if (this.name.Equals("NUMBER")
                    || this.name.Equals("INTEGER")
                    || this.name.Equals("BINARY_INTEGER"))
            {
                sb.Append("putn(" + source + ");\n");
            }
            else if (this.name.Equals("DATE"))
            {
                sb.Append("putd( " + source + ");\n");
            }
            else if (this.name.Equals("PL/SQL BOOLEAN"))
            {
                sb.Append("putn(case when " + source + " then 1 when not " + source + " then 0 else null end);\n");
            }
            else
            {
                throw new ApplicationException("unsupported base type");
            }
        }


        public override void genReadOutThing(StringBuilder sb, Counter counter, String target)
        {
            if (this.name.Equals("NUMBER")
                    || this.name.Equals("INTEGER")
                    || this.name.Equals("BINARY_INTEGER"))
            {
                sb.Append(target).Append(":= getn;\n");
            }
            else if (this.name.Equals("DATE"))
            {
                //sb.Append(target).Append(":= ad(ind); ind := ind+1;\n");
                sb.Append(target).Append(":= getd;\n");
            }
            else if (this.name.Equals("PL/SQL BOOLEAN"))
            {
                sb.Append(target).Append(":= 1=getn;\n");
            }
            else
            {
                throw new ApplicationException("unsupported base type");
            }
        }
    }

    class Varchar2Type : PLSQLType
    {
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

        public override void genWriteThing(StringBuilder sb, Counter counter, String source)
        {
            sb.Append("putv(" + source + ");\n");
        }

        public override void genReadOutThing(StringBuilder sb, Counter counter, String target)
        {
            sb.Append(target).Append(":= getv;\n");
        }
    }

    class RawType : PLSQLType
    {

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

        public override void genWriteThing(StringBuilder sb, Counter counter, String source)
        {
            sb.Append("putr(" + source + ");\n");
        }

        public override void genReadOutThing(StringBuilder sb, Counter counter, String target)
        {
            sb.Append(target).Append(":= getr;\n");
        }
    }

    class Field
    {
        public String name;
        public PLSQLType type;
    }

    class RecordType : PLSQLType
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

        public void readFromResArraysIntoDataRow(ResArrays a, DataRow r)
        {
            Dictionary<String, Object> m = new Dictionary<String, Object>();
            foreach (Field f in this.fields)
            {
                Object o = f.type.readFromResArrays(a);
                r[f.name] = o;
            }
        }

        public override void genWriteThing(StringBuilder sb, Counter counter, String source)
        {
            foreach (Field f in this.fields)
            {
                String a = source + "." + f.name;
                f.type.genWriteThing(sb, counter, a);
            }
        }

        public override void genReadOutThing(StringBuilder sb, Counter counter, String target)
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
    class TableType : PLSQLType
    {
        public String owner;
        public String package_;
        public String name;
        public PLSQLType slottype;

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

        public override void genWriteThing(StringBuilder sb, Counter counter, String source)
        {
            sb.Append(" if " + source + " is null then\n");
            sb.Append("    putn(null);\n");
            sb.Append("else \n");
            sb.Append("  putn(nvl(" + source + ".last, 0));\n");
            String index = "i" + counter.incrementAndGet();
            sb.Append("for " + index + " in 1 .. nvl(" + source + ".last,0) loop\n");
            this.slottype.genWriteThing(sb, counter, source + "(" + index + ")");
            sb.Append("end loop;\n");
            sb.Append("end if;\n");
        }

        public override void genReadOutThing(StringBuilder sb, Counter counter, String target)
        {
            sb.Append("size_ := getn;\n");
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

    class IndexByStringTableType : PLSQLType
    {

        public String owner;
        public String package_;
        public String name;
        public PLSQLType slottype;

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

        public override void genWriteThing(StringBuilder sb, Counter counter, String source)
        {
            sb.Append("  putn(" + source + ".count);\n");
            String index = "i" + counter.incrementAndGet();
            sb.Append("declare " + index + " varchar2(32000) := " + source + ".first;\n");
            sb.Append("begin\n");
            sb.Append(" loop\n");
            sb.Append("exit when " + index + " is null;\n");
            sb.Append("  putv(" + index + ");\n");
            this.slottype.genWriteThing(sb, counter, source + "(" + index + ")");
            sb.Append(" " + index + " := " + source + ".next(" + index + ");\n");
            sb.Append("end loop;\n");
            sb.Append("end;\n");
        }

        public override void genReadOutThing(StringBuilder sb, Counter counter, String target)
        {
            sb.Append("size_ := getn;\n");
            sb.Append("if size_ is null then\n");
            sb.Append(" null;\n");
            sb.Append("else\n");
            String index = "i" + counter.incrementAndGet();
            String key = "k" + counter.incrementAndGet();
            String newTarget = target + "(" + key + ")";
            sb.Append("declare " + key + " varchar2(32000);\n");
            sb.Append("begin\n");
            sb.Append("  for ").Append(index).Append(" in 1 .. size_ loop\n");
            sb.Append("  " + key + " := getv;\n");
            this.slottype.genReadOutThing(sb, counter, newTarget);
            sb.Append("end loop;\n");
            sb.Append("end;\n");
            sb.Append("end if;\n");
        }
    }

    class IndexByIntegerTableType : PLSQLType
    {
        public String owner;
        public String package_;
        public String name;
        public PLSQLType slottype;

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

        public override void genWriteThing(StringBuilder sb, Counter counter, String source)
        {
            sb.Append("  putn( " + source + ".count);\n");

            String index = "i" + counter.incrementAndGet();
            sb.Append("declare " + index + " integer := " + source + ".first;\n");
            sb.Append("begin\n");
            sb.Append(" loop\n");
            sb.Append("exit when " + index + " is null;\n");
            sb.Append("  putn(" + index + ");\n");
            this.slottype.genWriteThing(sb, counter, source + "(" + index + ")");
            sb.Append(" " + index + " := " + source + ".next(" + index + ");\n");
            sb.Append("end loop;\n");
            sb.Append("end;\n");
        }

        public override void genReadOutThing(StringBuilder sb, Counter counter, String target)
        {
            sb.Append("size_ := getn;\n");
            sb.Append("if size_ is null then\n");
            sb.Append(" null;\n");
            sb.Append("else\n");
            String index = "i" + counter.incrementAndGet();
            String key = "k" + counter.incrementAndGet();
            String newTarget = target + "(" + key + ")";
            sb.Append("declare " + key + " integer;\n");
            sb.Append("begin\n");
            sb.Append("  for ").Append(index).Append(" in 1 .. size_ loop\n");
            sb.Append("  " + key + " :=getn;\n");
            this.slottype.genReadOutThing(sb, counter, newTarget);
            sb.Append("end loop;\n");
            sb.Append("end;\n");
            sb.Append("end if;\n");
        }
    }

    class SysRefCursorType : PLSQLType
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

        private readonly Boolean returnAsDataTable;
        public SysRefCursorType(Boolean returnAsDataTable)
        {
            this.returnAsDataTable = returnAsDataTable;
        }

        public override void fillArgArrays(ArgArrays a, Object o)
        {
            throw new ApplicationException("sys_refcursor may not be an \"IN\" or \"IN OUT\" parameter");
        }

        public DataTable ReadAsDataTable(ResArrays a)
        {
            int colcount = (int)a.readDecimal();
            List<String> colnames = new List<String>();
            List<String> coltypes = new List<String>();
            var tab = new DataTable();
            for (int i = 0; i < colcount; i++)
            {
                colnames.Add(a.readString());
                coltypes.Add(a.readString());
                var col = new DataColumn();
                col.ColumnName = colnames[i];
                String typ = coltypes[i];
                switch (typ)
                {
                    case "N": col.DataType = typeof(Decimal); break;
                    case "V": col.DataType = typeof(string); break;
                    case "D": col.DataType = typeof(DateTime); break;
                    case "R": col.DataType = typeof(byte[]); break;
                    default: throw new ApplicationException("unknwon column type: " + typ);
                }
                tab.Columns.Add(col);
            }
            while (true)
            {
                if ((int)a.readDecimal() == 0)
                {
                    break;
                }
                var row = tab.NewRow();
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
                    else if (t.Equals("R"))
                    {
                        o = a.readRaw();
                    }
                    else
                    {
                        throw new ApplicationException("unknwon column type: " + t);
                    }
                    if (o == null)
                    {
                        row[i] = DBNull.Value;
                    }
                    else
                    {
                        row[i] = o;
                    }
                }
                tab.Rows.Add(row);
            }
            return tab;
        }

        public override Object readFromResArrays(ResArrays a)
        {
            if (this.returnAsDataTable)
            {
                return this.ReadAsDataTable(a);
            }
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
                    else if (t.Equals("R"))
                    {
                        o = a.readRaw();
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

        public override void genReadOutThing(StringBuilder sb, Counter counter, String target)
        {
            throw new ApplicationException("sys_refcursor may not be an \"IN\" or \"IN OUT\" parameter");
        }

        public override void genWriteThing(StringBuilder sb, Counter counter, String source)
        {
            // OK Type Codes: dbms_types.TYPECODE_???? is wrong, at least for RAW
            // so manual check:
            // 23 raw, 2 number, 1, 96 varchar2, 12 date, 100 binary_float, 101 binary_double
            sb.Append("declare h integer;\n");
            sb.Append(" t varchar2(100);\n");
            sb.Append(" rec_tab   DBMS_SQL.DESC_TAB;\n");
            sb.Append(" rec       DBMS_SQL.DESC_REC;\n");
            sb.Append(" x number;\n");
            sb.Append("num number;\n");
            sb.Append("dat date;\n");
            sb.Append("raww raw(32767);\n");
            sb.Append("varc varchar2(4000);\n");
            sb.Append(" col_cnt integer;\n");
            sb.Append("begin\n");
            sb.Append(" h := DBMS_SQL.TO_CURSOR_NUMBER (" + source + ");\n");
            sb.Append(" DBMS_SQL.DESCRIBE_COLUMNS(h, col_cnt, rec_tab);\n");
            sb.Append(" putn(col_cnt);\n");
            sb.Append(" for i in 1 .. rec_tab.last loop\n");
            sb.Append("  rec := rec_tab(i);\n");
            sb.Append("  putv(rec.col_name);\n");
            sb.Append("if rec.col_type = 12 then\n");
            sb.Append("        dbms_sql.define_column(h, i, dat);\n");
            sb.Append(" t:=t||'D';\n");
            sb.Append("elsif rec.col_type = 2 then\n");
            sb.Append("        dbms_sql.define_column(h, i, num);\n");
            sb.Append(" t:=t||'N';\n");
            sb.Append("elsif rec.col_type = 23  then\n");
            sb.Append("        dbms_sql.define_column_raw(h, i, raww,32767);\n");
            sb.Append(" t:=t||'R';\n");
            sb.Append("elsif rec.col_type in(1,96)  then\n");
            sb.Append("        dbms_sql.define_column(h, i, varc, 4000);\n");
            sb.Append(" t:=t||'V';\n");
            sb.Append(" else raise_application_error(-20000,'unknown type code for column '|| rec.col_name ||': '|| rec.col_type);");
            sb.Append("end if;");
            sb.Append("putv(substr(t,i,1));\n");
            sb.Append(" end loop;\n");
            sb.Append(" loop\n");
            sb.Append("      x := DBMS_SQL.FETCH_ROWS(h);\n");
            sb.Append("      exit when x = 0;\n");
            sb.Append("      putn(1);\n");
            sb.Append("      for i in 1 .. col_cnt loop\n");
            sb.Append("        case substr(t,i,1) \n");
            sb.Append("         when 'D' then\n");
            sb.Append("          DBMS_SQL.COLUMN_VALUE(h, i, dat);\n");
            sb.Append("          putd(dat);\n");
            sb.Append("        when 'N' then\n");
            sb.Append("          DBMS_SQL.COLUMN_VALUE(h, i, num);\n");
            sb.Append("          putn(num);\n");
            sb.Append("        when 'R' then\n");
            sb.Append("          DBMS_SQL.COLUMN_VALUE_raw(h, i, raww);\n");
            sb.Append("          putr(raww);\n");
            sb.Append("        when 'V' then\n");
            sb.Append("          DBMS_SQL.COLUMN_VALUE(h, i, varc);\n");
            sb.Append("          putv(varc);\n");
            sb.Append("         else raise_application_error(-20000,'BUG: unknown internal type code: '||t);\n");
            sb.Append("         end case;\n");
            sb.Append("      end loop;\n");
            sb.Append("    end loop;\n");
            sb.Append("      putn(0);\n");
            sb.Append("end;");
        }
    }

    class TypedRefCursorType : PLSQLType
    {
        public String owner;
        public String package_;
        public String name;
        public RecordType rectype;

        private readonly bool returnAsDataTable;

        public TypedRefCursorType(bool returnAsDataTable)
        {
            this.returnAsDataTable = returnAsDataTable;
        }

        public override String plsqlName()
        {
            return "sys_refcursor";
        }

        public override void fillArgArrays(ArgArrays a, Object o)
        {
            throw new ApplicationException("ref cursor may not be an \"IN\" or \"IN OUT\" parameter");
        }

        private DataTable readAsDataTableFromResArray(ResArrays a)
        {
            var tab = new DataTable();
            foreach (Field f in this.rectype.fields)
            {
                var col = new DataColumn(f.name);
                if (f.type is Varchar2Type)
                {
                    col.DataType = typeof(string);
                }
                else if (f.type is RawType)
                {
                    col.DataType = typeof(byte[]);
                }
                else if (f.type is NamedType)
                {
                    var nt = (NamedType)f.type;
                    if (nt.name.Equals("NUMBER") || nt.name.Equals("INTEGER") || nt.name.Equals("BINARY_INTEGER"))
                    {
                        col.DataType = typeof(decimal);
                    }
                    else if (nt.name.Equals("DATE"))
                    {
                        col.DataType = typeof(DateTime);
                    }
                    else
                    {
                        throw new ApplicationException("no support for column type in DataTable: " + f);
                    }
                }
                else
                {
                    throw new ApplicationException("no support for column type in DataTable: " + f);
                }
                tab.Columns.Add(col);
            }
            while (true)
            {
                if ((int)a.readDecimal() == 0)
                {
                    break;
                }
                var row = tab.NewRow();
                rectype.readFromResArraysIntoDataRow(a, row);
                tab.Rows.Add(row);
            }
            return tab;
        }

        public override Object readFromResArrays(ResArrays a)
        {
            if (this.returnAsDataTable)
            {
                return this.readAsDataTableFromResArray(a);
            }
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


        public override void genReadOutThing(StringBuilder sb, Counter counter, String target)
        {
            throw new ApplicationException("ref cursor may not be an \"IN\" or \"IN OUT\" parameter");
        }


        public override void genWriteThing(StringBuilder sb, Counter counter, String source)
        {
            sb.Append("declare r " + rectype.plsqlName() + ";\n");
            sb.Append("x integer;\n");
            sb.Append("begin\n");
            sb.Append("loop\n");
            sb.Append(" fetch " + source + " into r;\n");
            sb.Append("if " + source + "%notfound then\n");
            sb.Append("  exit;\n");
            sb.Append("end if;\n");
            sb.Append("putn(1);\n");
            rectype.genWriteThing(sb, counter, "r");
            sb.Append("end loop;\n");
            sb.Append("putn(0);\n");
            sb.Append("end;\n");
        }
    }
}

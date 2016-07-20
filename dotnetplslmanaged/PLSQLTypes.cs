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
                if (o == null|| DBNull.Value == o)
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
                if (o == null || DBNull.Value == o)
                {
                    a.addDateTime(null);
                }
                else
                {
                    a.addDateTime((DateTime?)o);
                }
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
            
            if (o == null|| DBNull.Value == o)
            {
                a.addString(null);
            }
            else
            {
                String s = (String)o;
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
         
            if (o == null||DBNull.Value == o)
            {
                a.addRaw(null);
            }
            else
            {
                byte[] b = (byte[])o;
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
            else if (o is DataTable)
            {
                if (!(this.slottype is RecordType)) 
                {
                    throw new ApplicationException("if datatable is given for array then array slotype must be record type");
                }
                var rt = (RecordType) this.slottype;
                var tab = (DataTable)o;
                a.addNumber(tab.Rows.Count);
                foreach (DataRow r in tab.Rows)
                {
                    foreach(Field f in rt.fields) {
                        f.type.fillArgArrays(a, r[f.name]);
                    }
                }
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

        public override void fillArgArrays(ArgArrays a, Object o)
        {
            throw new ApplicationException("sys_refcursor may not be an \"IN\" or \"IN OUT\" parameter");
        }

        public override Object readFromResArrays(ResArrays a)
        {
            throw new ApplicationException("not implmented for ref cursor");
        }

        public override void genReadOutThing(StringBuilder sb, Counter counter, String target)
        {
            throw new ApplicationException("sys_refcursor may not be an \"IN\" or \"IN OUT\" parameter");
        }

        public override void genWriteThing(StringBuilder sb, Counter counter, String source)
        {
            throw new ApplicationException("not implmented for ref cursor");
        }
    }

    class TypedRefCursorType : PLSQLType
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
            throw new ApplicationException("not implmented for ref cursor");
        }


        public override void genReadOutThing(StringBuilder sb, Counter counter, String target)
        {
            throw new ApplicationException("ref cursor may not be an \"IN\" or \"IN OUT\" parameter");
        }


        public override void genWriteThing(StringBuilder sb, Counter counter, String source)
        {
            throw new ApplicationException("not implmented for ref cursor");
        }
    }
}

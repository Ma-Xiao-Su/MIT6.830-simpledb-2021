package simpledb.common;

import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 目录（`Catalog`SimpleDB 中的类）由当前在数据库中的表和表的模式列表组成。
 * 您将需要支持添加新表以及获取有关特定表的信息的能力。与每个表相关联的是一个`TupleDesc`对象，它允许操作员确定表中字段的类型和数量。
 *
 * `Catalog`全局目录是为整个 SimpleDB 进程分配的单个实例。
 * 全局目录可以通过 方法检索`Database.getCatalog()`，全局缓冲池（ using `Database.getBufferPool()`）也是如此。
 *
 */

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * 目录跟踪数据库中所有可用的表及其关联的模式。
 * 目前，这是一个存根目录，用户程序必须先用表填充它，然后才能使用它 — —
 * 最终，它应该转换为从磁盘读取目录表的目录。
 *
 *
 * catalog 是 database中所有表的集合
 *
 * @Threadsafe
 */
public class Catalog {
    /**
     * Represents information about a table in the database
     */
    class Table {
        private final DbFile dbField;
        private final String name;
        private final String pKeyField;

        public Table(DbFile dbfile, String name, String pKeyField) {
            this.dbField = dbfile;
            this.name = name;
            this.pKeyField = pKeyField;
        }

        public DbFile getDbField() {
            return dbField;
        }

        public String getName() {
            return name;
        }

        public String getPKeyField() {
            return pKeyField;
        }
    }

    private final ConcurrentMap<Integer, Table> tableIdToTableMap;
    private final ConcurrentMap<String, Integer> nameToTableIdMap;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        tableIdToTableMap = new ConcurrentHashMap<>();
        nameToTableIdMap = new ConcurrentHashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        Table newTable = new Table(file, name, pkeyField);
        tableIdToTableMap.put(file.getId(), newTable);
        nameToTableIdMap.put(name, file.getId());
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null || !nameToTableIdMap.containsKey(name)) {
            throw new NoSuchElementException();
        }
        return nameToTableIdMap.get(name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        return getDatabaseFile(tableid).getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!tableIdToTableMap.containsKey(tableid)) {
            throw new NoSuchElementException();
        }
        return tableIdToTableMap.get(tableid).getDbField();
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        return tableIdToTableMap.get(tableid).getPKeyField();
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return nameToTableIdMap.values().iterator();
    }


    public String getTableName(int id) {
        // some code goes here
        return tableIdToTableMap.get(id).getName();
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        tableIdToTableMap.clear();
        nameToTableIdMap.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}


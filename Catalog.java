package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {
	private List<DbFile> tables;
	private List<String> names;
	private List<String> pKeys;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
    	tables =  new ArrayList<DbFile>();
    	names = new ArrayList<String>();
    	pKeys = new ArrayList<String>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        tables.add(file);
        names.add(name);
        pKeys.add(pkeyField);
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
    	// loop through all the things in the table and check if the name is the same
        for (int i = 0; i < tables.size(); i++) {
        	if (names.get(i) == name) {
        		return tables.get(i).getId();
        	}
        }
        throw new NoSuchElementException("Table does not exist!");
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
    	
    	// loop through the table we have and see if we can find the given id
        for (int i = 0; i < tables.size(); i++) {
        	DbFile current = tables.get(i);
        	if (current.getId() == tableid) {
        		// then return the tupledesc
        		return current.getTupleDesc();
        	}
        }
        throw new NoSuchElementException("Table does not exist!");
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
    	// loop through the table and check if the we can find the given id
        for (int i = 0; i < tables.size(); i++) {
        	DbFile current = tables.get(i);
        	if (current.getId() == tableid) {
        		// then return the current file we are at
        		return current;
        	}
        }
        throw new NoSuchElementException("Table does not exist!");
    }

    public String getPrimaryKey(int tableid) {
    	// loop through all the things in the table and find the table with the given id
        for (int i = 0; i < tables.size(); i++) {
        	DbFile current = tables.get(i);
        	// then return the primary key of that table
        	if (current.getId() == tableid) {
        		return pKeys.get(i);
        	}
        }
        throw new NoSuchElementException("Key does not exist!");
    }

    public Iterator<Integer> tableIdIterator() {
    	// we need to make a custom iterator
    	Iterator<Integer> iterator = new Iterator<Integer>() {
    		public int current = 0;
    		
    		// need to check if there is a next
    		public boolean hasNext() {
    			int next = current + 1;
    			return (next == tables.size());
    		};
    		
    		// actually go to the next item in the table
    		public Integer next() {
    			if (hasNext()) {
    				current++;
    				return tables.get(current).getId();
    			} else {
    				throw new NoSuchElementException("Next does not exist!");
    			}
    		};
    		public void remove() {
    			// do nothing
    		};
    	};
    	return iterator;
    }

    public String getTableName(int id) {
    	// just loop through the table and find the table with the right id
        for (int i = 0; i < tables.size(); i++) {
        	if (tables.get(i).getId() == id) {
        		// and return the name of that table
        		return names.get(i);
        	}
        }
        throw new NoSuchElementException("No table with that id!");
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        tables.clear();
        names.clear();
        pKeys.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
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


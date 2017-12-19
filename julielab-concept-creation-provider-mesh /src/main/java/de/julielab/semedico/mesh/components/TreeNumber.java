package de.julielab.semedico.mesh.components;

/**
 * For importing and exporting (only!), instances of this class represent a tree
 * number of the MeSH.
 * 
 * A tree number is expected to have the following format: (alphaNum.)*alphaNum
 * where alphaNum is an alpha-numeric string. e.g. D011.231.231 or
 * 098316323.23123.123123.12.4.1.23.3.3
 * 
 * @author Philipp Lucas
 */
public class TreeNumber {
    private String number;

    public TreeNumber(String number) {
        this.number = number;
    }    
    
    public TreeNumber(TreeNumber number) {
    	this.number = number.number;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }
    
    public String[] getPartialNumbers() {
        return getNumber().split("\\.");
    }
    
    /**
     * @return Returns the "last" partial number of the tree number, i.e. the number in the lowest hierarchy level.
     * e.g.: if the tree number is 1.2.3.4.5 then 5 will be returned.
     */
    public String getLastPartialNumber() {
        String[] partialNumbers = getPartialNumbers();
        return partialNumbers[partialNumbers.length-1];
    }
    
    /**
     * @return Returns the "first" partial number of the tree number, i.e. the number in the highest hierarchy level.
     * e.g.: if the tree number is 1.2.3.4.5 then 1 will be returned.
     */  
	public String getFirstPartialNumber() {
		 String[] partialNumbers = getPartialNumbers();
	     return partialNumbers[0];
	}
    
    /**
     * @return Returns the tree-number of the parent, or null if there is no parent.
     */
    public TreeNumber getParentNumber() {
        if (hasParentNumber()) {
            return new TreeNumber(number.substring(0, number.lastIndexOf(".")));
        } else {
            return null;
        }
    }
    
    public boolean hasParentNumber() {
        return this.number.contains(".");
    }
    
    /**
     * @param partialNr the partial nr to check for
     * @return Returns true if the first element of getPartialNumbers() equals partialNr; false else.
     */
    public boolean firstPartialNrIs(String partialNr) {
        String[] partNrs = getPartialNumbers();
        if(partNrs.length>=1){
            return partialNr.equals(partNrs[0]);
        } else {
            return false;
        }
    }
    
    /**
     * Note that this will return 1 for root but also 1 for direct children of root. This is because 
     * it simply counts the number of "."'s in it.
     * @return Returns the "depth of this tree number", i.e. the number of "." in it plus 1.
     */
    public int getLevel () {
    	int cnt=1;
    	for(int i=0;i<number.length();i++) {
    		if(number.charAt(i) == '.') {
    			cnt++;
    		}
    	}
    	return cnt;
    }
    
    @Override
    public String toString() {
        return getNumber();
    }
    
    @Override
    public boolean equals(Object t) {
        if(t instanceof TreeNumber) {
            return ((TreeNumber)t).getNumber().equals(number);
        } else {
            return super.equals(t);
        }
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 59 * hash + (this.number != null ? this.number.hashCode() : 0);
        return hash;
    }
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.sf.jsqlparser.statement.select;

import net.sf.jsqlparser.schema.Table;

import java.util.List;

/**
 *
 * @author ab
 */
public class Into {
    private List<String> commentsComma;
    private List<Table> tables;
    private String commentInto;
    
     @Override
    public String toString() {
        return (getCommentInto() != null ? getCommentInto()+" " : "") + "INTO " 
                + PlainSelect.getStringListWithCommaComment(tables,commentsComma,true,false,null);
    }

    /**
     * @return the commentsComma
     */
    public List<String> getCommentsComma() {
        return commentsComma;
    }

    /**
     * @param commentsComma the commentsComma to set
     */
    public void setCommentsComma(List<String> commentsComma) {
        this.commentsComma = commentsComma;
    }

    /**
     * @return the tables
     */
    public List<Table> getTables() {
        return tables;
    }

    /**
     * @param tables the tables to set
     */
    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    /**
     * @return the commentInto
     */
    public String getCommentInto() {
        return commentInto;
    }

    /**
     * @param commentInto the commentInto to set
     */
    public void setCommentInto(String commentInto) {
        this.commentInto = commentInto;
    }
}

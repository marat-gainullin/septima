package net.sf.jsqlparser.statement.select;

import net.sf.jsqlparser.expression.NamedParameter;

/**
 * A limit clause in the form [LIMIT {[offset,] row_count) | (row_count | ALL) OFFSET offset}]
 */
public class Limit {

    private long offset;
    private long rowCount;
    private boolean offsetJdbcParameter;
    private boolean rowCountJdbcParameter;
    private NamedParameter offsetNamedParameter;
    private NamedParameter rowCountNamedParameter;
    private boolean limitAll;
    private boolean comma = false;
    private String commentLimit;
    private String commentOffset;
    private String commentOffsetValue;
    private String commentLimitValue;
    private String commentAll;
    private String commentComma;
    private String commentAfterCommaValue;

    public long getOffset() {
        return offset;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setOffset(long l) {
        offset = l;
    }

    public void setRowCount(long l) {
        rowCount = l;
    }

    public boolean isOffset() {
        return offsetJdbcParameter || offsetNamedParameter != null || offset != 0;
    }

    public boolean isRowCount() {
        return rowCountJdbcParameter || rowCountNamedParameter != null || rowCount != 0;
    }

    public boolean isOffsetJdbcParameter() {
        return offsetJdbcParameter;
    }

    public boolean isRowCountJdbcParameter() {
        return rowCountJdbcParameter;
    }

    public void setOffsetJdbcParameter(boolean b) {
        offsetJdbcParameter = b;
    }

    public void setRowCountJdbcParameter(boolean b) {
        rowCountJdbcParameter = b;
    }

    public NamedParameter getOffsetNamedParameter() {
        return offsetNamedParameter;
    }

    public void setOffsetNamedParameter(NamedParameter offsetNamedParameter) {
        this.offsetNamedParameter = offsetNamedParameter;
    }

    public NamedParameter getRowCountNamedParameter() {
        return rowCountNamedParameter;
    }

    public void setRowCountNamedParameter(NamedParameter rowCountNamedParameter) {
        this.rowCountNamedParameter = rowCountNamedParameter;
    }

    /**
     * @return true if the limit is "LIMIT ALL [OFFSET ...])
     */
    public boolean isLimitAll() {
        return limitAll;
    }

    public void setLimitAll(boolean b) {
        limitAll = b;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (limitAll || rowCountNamedParameter != null || rowCountJdbcParameter || rowCount != 0) {
            builder
                    .append(commentLimit != null ? " " + commentLimit : "")
                    .append(" LIMIT");
        }
        if (comma) {
            builder
                    .append(commentLimitValue != null ? " " + commentLimitValue : "");

            builder.append(" ");
            if (offsetJdbcParameter) {
                builder.append("?");
            } else if (offsetNamedParameter != null) {
                builder.append(offsetNamedParameter.toString());
            } else if (offset != 0) {
                builder.append(offset);
            }

            builder
                    .append(commentComma != null ? " " + commentComma : "")
                    .append(",")
                    .append(commentAfterCommaValue != null ? " " + commentAfterCommaValue : "");

            builder.append(" ");
            if (rowCountJdbcParameter) {
                builder.append("?");
            } else if (rowCountNamedParameter != null) {
                builder.append(rowCountNamedParameter.toString());
            } else if (rowCount != 0) {
                builder.append(rowCount);
            }
        } else {
            if (limitAll) {
                builder
                        .append(commentAll != null ? " " + commentAll : "")
                        .append(" ALL");
            } else if (rowCountNamedParameter != null || rowCountJdbcParameter || rowCount != 0) {
                builder
                        .append(commentLimitValue != null ? " " + commentLimitValue : "");

                builder.append(" ");
                if (rowCountJdbcParameter) {
                    builder.append("?");
                } else if (rowCountNamedParameter != null) {
                    builder.append(rowCountNamedParameter.toString());
                } else if (rowCount != 0) {
                    builder.append(rowCount);
                }
            }

            if (offsetJdbcParameter || offsetNamedParameter != null || offset != 0) {
                builder
                        .append(commentOffset != null ? " " + commentOffset : "")
                        .append(" OFFSET")
                        .append(commentOffsetValue != null ? " " + commentOffsetValue : "");

                builder.append(" ");
                if (offsetJdbcParameter) {
                    builder.append("?");
                } else if (offsetNamedParameter != null) {
                    builder.append(offsetNamedParameter.toString());
                } else if (offset != 0) {
                    builder.append(offset);
                }
            }
        }
        return builder.toString();
    }

    /**
     * @return the commentLimit
     */
    public String getCommentLimit() {
        return commentLimit;
    }

    /**
     * @param commentLimit the commentLimit transform set
     */
    public void setCommentLimit(String commentLimit) {
        this.commentLimit = commentLimit;
    }

    /**
     * @return the commentOffset
     */
    public String getCommentOffset() {
        return commentOffset;
    }

    /**
     * @param commentOffset the commentOffset transform set
     */
    public void setCommentOffset(String commentOffset) {
        this.commentOffset = commentOffset;
    }

    /**
     * @return the commentOffsetValue
     */
    public String getCommentOffsetValue() {
        return commentOffsetValue;
    }

    /**
     * @param commentOffsetValue the commentOffsetValue transform set
     */
    public void setCommentOffsetValue(String commentOffsetValue) {
        this.commentOffsetValue = commentOffsetValue;
    }

    /**
     * @return the commentLimitValue
     */
    public String getCommentLimitValue() {
        return commentLimitValue;
    }

    /**
     * @param commentLimitValue the commentLimitValue transform set
     */
    public void setCommentLimitValue(String commentLimitValue) {
        this.commentLimitValue = commentLimitValue;
    }

    /**
     * @return the comma
     */
    public boolean isComma() {
        return comma;
    }

    /**
     * @param comma the comma transform set
     */
    public void setComma(boolean comma) {
        this.comma = comma;
    }

    /**
     * @return the commentAll
     */
    public String getCommentAll() {
        return commentAll;
    }

    /**
     * @param commentAll the commentAll transform set
     */
    public void setCommentAll(String commentAll) {
        this.commentAll = commentAll;
    }

    /**
     * @return the commentComma
     */
    public String getCommentComma() {
        return commentComma;
    }

    /**
     * @param commentComma the commentComma transform set
     */
    public void setCommentComma(String commentComma) {
        this.commentComma = commentComma;
    }

    /**
     * @return the commentAfterCommaValue
     */
    public String getCommentAfterCommaValue() {
        return commentAfterCommaValue;
    }

    /**
     * @param commentAfterCommaValue the commentAfterCommaValue transform set
     */
    public void setCommentAfterCommaValue(String commentAfterCommaValue) {
        this.commentAfterCommaValue = commentAfterCommaValue;
    }
}

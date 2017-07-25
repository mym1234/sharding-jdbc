/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.rdb.sharding.parsing.parser.dialect.mysql;

import com.dangdang.ddframe.rdb.sharding.parsing.lexer.dialect.mysql.MySQLKeyword;
import com.dangdang.ddframe.rdb.sharding.parsing.lexer.dialect.oracle.OracleKeyword;
import com.dangdang.ddframe.rdb.sharding.parsing.lexer.token.Assist;
import com.dangdang.ddframe.rdb.sharding.parsing.lexer.token.DefaultKeyword;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.SQLParser;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.exception.SQLParsingUnsupportedException;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.select.AbstractSelectParser;

public class MySQLSelectParser extends AbstractSelectParser {
    
    public MySQLSelectParser(final SQLParser sqlParser) {
        super(sqlParser);
    }

    /**
     * 查询 SQL 解析
     * SELECT Syntax：https://dev.mysql.com/doc/refman/5.7/en/select.html
     */
//    SELECT
//    [ALL | DISTINCT | DISTINCTROW ]
//            [HIGH_PRIORITY]
//            [STRAIGHT_JOIN]
//            [SQL_SMALL_RESULT] [SQL_BIG_RESULT] [SQL_BUFFER_RESULT]
//            [SQL_CACHE | SQL_NO_CACHE] [SQL_CALC_FOUND_ROWS]
//            select_expr [, select_expr ...]
//            [FROM table_references
//              [PARTITION partition_list]
//            [WHERE where_condition]
//            [GROUP BY {col_name | expr | position}
//              [ASC | DESC], ... [WITH ROLLUP]]
//            [HAVING where_condition]
//            [ORDER BY {col_name | expr | position}
//              [ASC | DESC], ...]
//            [LIMIT {[offset,] row_count | row_count OFFSET offset}]
//            [PROCEDURE procedure_name(argument_list)]
//            [INTO OUTFILE 'file_name'
//               [CHARACTER SET charset_name]
//               export_options
//              | INTO DUMPFILE 'file_name'
//              | INTO var_name [, var_name]]
//            [FOR UPDATE | LOCK IN SHARE MODE]]
    @Override
    public void query() {
        if (getSqlParser().equalAny(DefaultKeyword.SELECT)) {
            getSqlParser().getLexer().nextToken();
            parseDistinct();
            getSqlParser().skipAll(MySQLKeyword.HIGH_PRIORITY, DefaultKeyword.STRAIGHT_JOIN, MySQLKeyword.SQL_SMALL_RESULT, MySQLKeyword.SQL_BIG_RESULT, MySQLKeyword.SQL_BUFFER_RESULT,
                    MySQLKeyword.SQL_CACHE, MySQLKeyword.SQL_NO_CACHE, MySQLKeyword.SQL_CALC_FOUND_ROWS);
            // 解析 查询字段
            parseSelectList();
            // 跳到 FROM 处
            skipToFrom();
        }
        // 解析 表（JOIN ON / FROM 单&多表）
        parseFrom();
        // 解析 WHERE 条件
        parseWhere();
        // 解析 Group By 和 Having（目前不支持）条件
        parseGroupBy();
        // 解析 Order By 条件
        parseOrderBy();
        // 解析 分页 Limit 条件
        parseLimit();
        // [PROCEDURE] 暂不支持
        if (getSqlParser().equalAny(DefaultKeyword.PROCEDURE)) {
            throw new SQLParsingUnsupportedException(getSqlParser().getLexer().getCurrentToken().getType());
        }
        // TODO 疑问：待定
        queryRest();
    }
    
    private void parseLimit() {
        if (getSqlParser().equalAny(MySQLKeyword.LIMIT)) {
            ((MySQLParser) getSqlParser()).parseLimit(getSelectStatement(), getParametersIndex());
        }
    }
    
    private void skipToFrom() {
        while (!getSqlParser().equalAny(DefaultKeyword.FROM) && !getSqlParser().equalAny(Assist.END)) {
            getSqlParser().getLexer().nextToken();
        }
    }
    
    @Override
    protected void parseJoinTable() {
        if (getSqlParser().equalAny(DefaultKeyword.USING)) {
            return;
        }
        if (getSqlParser().equalAny(DefaultKeyword.USE)) {
            getSqlParser().getLexer().nextToken();
            parseIndexHint();
        }
        if (getSqlParser().equalAny(OracleKeyword.IGNORE)) {
            getSqlParser().getLexer().nextToken();
            parseIndexHint();
        }
        if (getSqlParser().equalAny(OracleKeyword.FORCE)) {
            getSqlParser().getLexer().nextToken();
            parseIndexHint();
        }
        super.parseJoinTable();
    }

    private void parseIndexHint() {
        if (getSqlParser().equalAny(DefaultKeyword.INDEX)) {
            getSqlParser().getLexer().nextToken();
        } else {
            getSqlParser().accept(DefaultKeyword.KEY);
        }
        if (getSqlParser().equalAny(DefaultKeyword.FOR)) {
            getSqlParser().getLexer().nextToken();
            if (getSqlParser().equalAny(DefaultKeyword.JOIN)) {
                getSqlParser().getLexer().nextToken();
            } else if (getSqlParser().equalAny(DefaultKeyword.ORDER)) {
                getSqlParser().getLexer().nextToken();
                getSqlParser().accept(DefaultKeyword.BY);
            } else {
                getSqlParser().accept(DefaultKeyword.GROUP);
                getSqlParser().accept(DefaultKeyword.BY);
            }
        }
        getSqlParser().skipParentheses();
    }
}

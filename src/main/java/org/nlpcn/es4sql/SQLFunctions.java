package org.nlpcn.es4sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.common.collect.Tuple;
import org.nlpcn.es4sql.domain.KVValue;

import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Created by allwefantasy on 8/19/16.
 */
public class SQLFunctions {

    // Groovy Built In Functions
    public final static Set<String> buildInFunctions = Sets.newHashSet("exp", "log", "log10", "sqrt", "cbrt", "ceil",
            "floor", "rint", "pow", "round", "random", "abs", // nummber
            // operator
            "split", "concat_ws", "substring", "trim", "tostring", "concat", "length", // string
            // operator
            "add", "multiply", "divide", "subtract", "modulus", // binary
                                                                // operator
            "field", "date_format", "double", "todate");

    public static Tuple<String, String> function(String methodName, List<KVValue> paramers, String name) {
        Tuple<String, String> functionStr = null;
        switch (methodName) {
        case "split":
            if (paramers.size() == 3) {
                functionStr = split(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                        Util.expr2Object((SQLExpr) paramers.get(1).value).toString(),
                        Integer.parseInt(Util.expr2Object((SQLExpr) paramers.get(2).value).toString()), name);
            } else {
                functionStr = split(paramers.get(0).value.toString(), paramers.get(1).value.toString(), name);
            }

            break;

        case "concat_ws":
            List<SQLExpr> result = Lists.newArrayList();
            for (int i = 1; i < paramers.size(); i++) {
                result.add((SQLExpr) paramers.get(i).value);
            }
            functionStr = concat_ws(paramers.get(0).value.toString(), result, name);

            break;
        case "concat":
            result = Lists.newArrayList();
            for (int i = 0; i < paramers.size(); i++) {
                result.add((SQLExpr) paramers.get(i).value);
            }
            functionStr = concat(result);

            break;
        case "floor":
            functionStr = floor(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
            break;

        case "date_format":
            functionStr = date_format(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                    Util.expr2Object((SQLExpr) paramers.get(1).value).toString(), name);
            break;

        case "round":
            functionStr = round(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
            break;
        case "log":
            functionStr = log(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
            break;

        case "log10":
            functionStr = log10(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
            break;

        case "sqrt":
            functionStr = sqrt(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
            break;

        case "substring":
            functionStr = substring(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                    Integer.parseInt(Util.expr2Object((SQLExpr) paramers.get(1).value).toString()),
                    Integer.parseInt(Util.expr2Object((SQLExpr) paramers.get(2).value).toString()), name);
            break;
        case "trim":
            functionStr = trim(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
            break;
        case "tostring":
            functionStr = toString(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
            break;
        case "length":
            functionStr = length(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
            break;
        case "add":
            functionStr = add((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
            break;

        case "subtract":
            functionStr = subtract((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
            break;
        case "divide":
            functionStr = divide((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
            break;

        case "multiply":
            functionStr = multiply((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
            break;
        case "modulus":
            functionStr = modulus((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
            break;

        case "field":
            functionStr = field(Util.expr2Object((SQLExpr) paramers.get(0).value).toString());
            break;
        case "double":
            functionStr = toDouble(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
            break;
        case "todate":
            functionStr = toDate(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                    Util.expr2Object((SQLExpr) paramers.get(1).value).toString(), name);
            break;
        default:

        }
        return functionStr;
    }

    private static Tuple<String, String> concat(List<SQLExpr> result) {
        String name = "concat_" + random();

        StringBuilder sb = new StringBuilder();
        sb.append("doc['" + result.get(0).toString() + "'].value");
        for (int i = 1; i < result.size(); i++) {
            SQLExpr sqlExpr = result.get(i);
            sb.append(".concat(doc['" + sqlExpr.toString() + "'].value)");
        }

        return new Tuple<String, String>(name, "def " + name + " = " + sb.toString());
    }

    public static String random() {
        return Math.abs(new Random().nextInt()) + "";
    }

    public static Tuple<String, String> concat_ws(String split, List<SQLExpr> columns, String valueName) {
        String name = "concat_ws_" + random();
        List<String> result = Lists.newArrayList();

        for (SQLExpr column : columns) {
            String strColumn = Util.expr2Object(column).toString();
            if (strColumn.startsWith("def ")) {
                result.add(strColumn);
            } else if (isProperty(column)) {
                result.add("doc['" + strColumn + "'].value");
            } else {
                result.add("'" + strColumn + "'");
            }

        }
        return new Tuple<String, String>(name, "def " + name + " =" + Joiner.on("+ " + split + " +").join(result));

    }

    // split(Column str, java.lang.String pattern)
    public static Tuple<String, String> split(String strColumn, String pattern, int index, String valueName) {
        String name = "split_" + random();
        if (valueName == null) {
            return new Tuple<String, String>(name,
                    "def " + name + " = doc['" + strColumn + "'].value.split('" + pattern + "')[" + index + "]");
        } else {
            return new Tuple<String, String>(name,
                    strColumn + "; def " + name + " = " + valueName + ".split('" + pattern + "')[" + index + "]");
        }

    }

    public static Tuple<String, String> date_format(String strColumn, String pattern, String valueName) {
        String name = "date_format_" + random();
        if (valueName == null) {
            return new Tuple<String, String>(name, "def " + name + " = new Date(doc['" + strColumn
                    + "'].value - 8*1000*60*60).format('" + pattern + "') ");
        } else {
            return new Tuple<String, String>(name, strColumn + "; def " + name + " = new Date(" + valueName
                    + " - 8*1000*60*60).format('" + pattern + "')");
        }

    }

    public static Tuple<String, String> toDate(String strColumn, String pattern, String valueName) {
        String name = "todate_" + random();
        if (valueName == null) {
            return new Tuple<String, String>(name,
                    "def " + name + " = Date.parse('" + pattern + "', doc['" + strColumn + "'].value) ");
        } else {
            return new Tuple<String, String>(name,
                    strColumn + "; def " + name + " = Date.parse('" + pattern + "'," + valueName + ") ");
        }

    }

    public static Tuple<String, String> add(SQLExpr a, SQLExpr b) {
        return binaryOpertator("add", "+", a, b);
    }

    public static Tuple<String, String> modulus(SQLExpr a, SQLExpr b) {
        return binaryOpertator("modulus", "%", a, b);
    }

    public static Tuple<String, String> field(String strColumn) {
        String name = "field_" + random();
        return new Tuple<String, String>(name, "def " + name + " = " + "doc['" + strColumn + "'].value");
    }

    public static Tuple<String, String> toDouble(String strColumn, String valueName) {
        String name = "double_" + random();
        if (valueName == null) {
            return new Tuple<String, String>(name, "def " + name + " = 0; if(!doc['" + strColumn + "'].empty) " + name
                    + "=doc['" + strColumn + "'].value.toDouble()");
        } else {
            return new Tuple<String, String>(name,
                    strColumn + "; def " + name + " = 0; if(" + valueName + "!=null) " + valueName + ".toDouble()");
        }

    }

    public static Tuple<String, String> subtract(SQLExpr a, SQLExpr b) {
        return binaryOpertator("subtract", "-", a, b);
    }

    public static Tuple<String, String> multiply(SQLExpr a, SQLExpr b) {
        return binaryOpertator("multiply", "*", a, b);
    }

    public static Tuple<String, String> divide(SQLExpr a, SQLExpr b) {
        return binaryOpertator("divide", "/", a, b);
    }

    public static Tuple<String, String> binaryOpertator(String methodName, String operator, SQLExpr a, SQLExpr b) {
        String name = methodName + "_" + random();

        return new Tuple<String, String>(name, scriptDeclare(a) + scriptDeclare(b) + convertType(a) + convertType(b)
                + " def " + name + " = " + extractName(a) + " " + operator + " " + extractName(b));
    }

    private static boolean isProperty(SQLExpr expr) {
        return (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr
                || expr instanceof SQLVariantRefExpr);
    }

    private static String scriptDeclare(SQLExpr a) {
        if (isProperty(a) || a instanceof SQLNumericLiteralExpr)
            return "";
        else
            return Util.expr2Object(a).toString() + ";";
    }

    private static String extractName(SQLExpr script) {
        if (isProperty(script))
            return "doc['" + script + "'].value";
        String scriptStr = Util.expr2Object(script).toString();
        String[] variance = scriptStr.split(";");
        String newScript = variance[variance.length - 1];
        if (newScript.trim().startsWith("def ")) {
            // for now ,if variant is string,then change to double.
            return newScript.trim().substring(4).split("=")[0].trim();
        } else if (newScript.trim().startsWith("if")) {
            int start = newScript.indexOf(")");
            int end = newScript.indexOf("=");
            return newScript.substring(start + 1, end).trim();
        } else
            return scriptStr;
    }

    // cast(year as int)

    private static String convertType(SQLExpr script) {
        String[] variance = Util.expr2Object(script).toString().split(";");
        String newScript = variance[variance.length - 1];
        if (newScript.trim().startsWith("def ")) {
            // for now ,if variant is string,then change to double.
            String temp = newScript.trim().substring(4).split("=")[0].trim();
            return " if(" + temp + " instanceof String) " + temp + "=" + temp.trim() + ".toDouble(); ";
        } else
            return "";
    }

    public static Tuple<String, String> log(String strColumn, String valueName) {
        return mathSingleValueTemplate("log", strColumn, valueName);
    }

    public static Tuple<String, String> log10(String strColumn, String valueName) {
        return mathSingleValueTemplate("log10", strColumn, valueName);
    }

    public static Tuple<String, String> sqrt(String strColumn, String valueName) {
        return mathSingleValueTemplate("sqrt", strColumn, valueName);
    }

    public static Tuple<String, String> round(String strColumn, String valueName) {
        return mathSingleValueTemplate("round", strColumn, valueName);
    }

    public static Tuple<String, String> trim(String strColumn, String valueName) {
        return strSingleValueTemplate("trim", strColumn, valueName);
    }

    public static Tuple<String, String> toString(String strColumn, String valueName) {
        return strSingleValueTemplate("toString", strColumn, valueName);
    }

    public static Tuple<String, String> length(String strColumn, String valueName) {
        return strSingleValueTemplate("length", strColumn, valueName);
    }

    public static Tuple<String, String> mathSingleValueTemplate(String methodName, String strColumn, String valueName) {
        String name = methodName + "_" + random();
        if (valueName == null) {
            return new Tuple<String, String>(name,
                    "def " + name + " = " + methodName + "(doc['" + strColumn + "'].value)");
        } else {
            return new Tuple<String, String>(name,
                    strColumn + ";def " + name + " = " + methodName + "(" + valueName + ")");
        }
    }

    public static Tuple<String, String> strSingleValueTemplate(String methodName, String strColumn, String valueName) {
        String name = methodName + "_" + random();
        if (valueName == null) {
            // return new Tuple(name, "def " + name + " = doc['" + strColumn +
            // "'].value." + methodName + "()");
            return new Tuple<String, String>(name, "def " + name + "=null;if(!doc['" + strColumn + "'].empty) " + name
                    + "=doc['" + strColumn + "'].value." + methodName + "()");
        } else {
            return new Tuple<String, String>(name,
                    strColumn + "; def " + name + " = " + valueName + "." + methodName + "()");
        }
    }

    public static Tuple<String, String> floor(String strColumn, String valueName) {
        return mathSingleValueTemplate("floor", strColumn, valueName);
    }

    // substring(Column str, int pos, int len)
    public static Tuple<String, String> substring(String strColumn, int pos, int len, String valueName) {
        String name = "substring_" + random();
        if (valueName == null) {
            // return new Tuple(name, "def " + name + " = doc['" + strColumn +
            // "'].value.substring(" + pos + "," + len + ")");
            return new Tuple<String, String>(name, "def " + name + "=null;if(!doc['" + strColumn + "'].empty) " + name
                    + "=doc['" + strColumn + "'].value.substring(" + pos + "," + len + ")");
        } else {
            return new Tuple<String, String>(name, strColumn + ";def " + name + " =null;if( " + valueName + "!=null) "
                    + valueName + ".substring(" + pos + "," + len + ")");
        }

    }

    // split(Column str, java.lang.String pattern)
    public static Tuple<String, String> split(String strColumn, String pattern, String valueName) {
        String name = "split_" + random();
        if (valueName == null) {
            return new Tuple<String, String>(name,
                    "def " + name + " = doc['" + strColumn + "'].value.split('" + pattern + "')");
        } else {
            return new Tuple<String, String>(name,
                    strColumn + "; def " + name + " = " + valueName + ".split('" + pattern + "')");
        }

    }

}

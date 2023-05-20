package database;

import java.util.*;
import java.util.function.Predicate;

class TableImpl implements Table {

    String name;
    List<ColumnImpl> tablecol = new ArrayList<>();

    @Override
    public Table crossJoin(Table rightTable) {
        return null;
    }

    @Override
    public Table innerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public Table outerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public Table fullOuterJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void show() {
        // 테이블 헤더 + 내용 출력
        int tableLength = tablecol.size();
        int cellLength = 0;
        String[][] temp = new String[tableLength][];
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tableLength]);
        //배열에 문자열 삽입, column 길이 구하기
        for (int j = 0; j < colArr.length; j++) {
            ColumnImpl tmp = colArr[j];
            temp[j] = new String[tmp.cell.size()];
            String[] arr = tmp.cell.toArray(new String[tmp.cell.size()]);
            temp[j] = new String[arr.length + 1];
            temp[j][0] = tmp.name;
            for (int k = 0; k < arr.length; k++) {
                temp[j][k + 1] = arr[k];
            }
            cellLength = arr.length + 1;
        }
        int[] columnsMaxLength = new int[tableLength];
        for (int i = 0; i < tableLength; i++) {
            columnsMaxLength[i] = 0;
            for (int j = 0; j < cellLength; j++) {
                if (temp[i][j] != null && temp[i][j].length() > columnsMaxLength[i]) {
                    columnsMaxLength[i] = temp[i][j].length();
                }
            }
        }
        //출력
        for (int i = 0; i < cellLength; i++) {
            for (int j = 0; j < tableLength; j++) {
                System.out.printf(" %" + columnsMaxLength[j] + "s|", temp[j][i]);
            }
            System.out.print("\n");
        }
    }

    @Override
    public void describe() {
        int I = 0;
        int S = 0;
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        //여기 Table이 아니라 TableImpl이 출력됨 수정
        Class[] q = getClass().getInterfaces();
        System.out.println("<" + q[0].getName() + "@" + Integer.toHexString(hashCode()) + ">");
        System.out.println("RangeIndex: " + colArr[0].cell.size() + " entries, 0 to " + (colArr[0].cell.size() - 1));
        System.out.println("Data columns (total " + colArr.length + " columns):");
        String[][] tmp = new String[colArr.length + 1][4];
        tmp[0][0] = "#";
        tmp[0][1] = "Column";
        tmp[0][2] = "Non-Null Count";
        tmp[0][3] = "Dtype";
        for (int i = 0; i < colArr.length; i++) {
            tmp[i + 1][0] = String.valueOf(i);
            tmp[i + 1][1] = colArr[i].name;
            String[] arr = colArr[i].cell.toArray(new String[colArr[i].cell.size()]);
            int cnt = 0;
            for (int k = 0; k < arr.length; k++) {
                if (arr[k] != null) {
                    cnt++;
                }
            }
            tmp[i + 1][2] = String.valueOf(cnt);
            tmp[i + 1][2] = tmp[i + 1][2].concat(" non-null");
            if (arr[1].matches("[+-]?\\d*(\\.\\d+)?")) {
                tmp[i + 1][3] = "int";
                I++;
            } else {
                tmp[i + 1][3] = "String";
                S++;
            }
        }
        int[] max = new int[3];
        for (int i = 0; i < 3; i++) {
            max[i] = 0;
            for (int j = 0; j < colArr.length + 1; j++) {
                if (tmp[j][i].length() > max[i]) {
                    max[i] = tmp[j][i].length();
                }
            }
        }
        for (int i = 0; i < colArr.length + 1; i++) {
            for (int j = 0; j < 3; j++) {
                System.out.printf("%" + max[j] + "s", tmp[i][j]);
                System.out.print(" |");
            }
            System.out.print(tmp[i][3]);
            System.out.println("");
        }
        System.out.println("dtypes: int(" + I + "), String(" + S + ")");
    }

    @Override
    public Table head() {
        int lineCount = 5;
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        ColumnImpl[] a = new ColumnImpl[colArr.length];
        for (int i = 0; i < colArr.length; i++) {
            a[i] = new ColumnImpl();
            String[] b = colArr[i].cell.toArray(new String[colArr[i].cell.size()]);
            a[i].name = colArr[i].name;
            if (lineCount > b.length) {
                lineCount = b.length;
            }
            for (int j = 0; j < lineCount; j++) {
                a[i].cell.add(b[j]);
            }
        }
        TableImpl t = new TableImpl();
        t.name = this.name;
        for (int i = 0; i < a.length; i++) {
            t.tablecol.add(a[i]);
        }
        return t;
    }

    @Override
    public Table head(int lineCount) {
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        ColumnImpl[] a = new ColumnImpl[colArr.length];
        for (int i = 0; i < colArr.length; i++) {
            a[i] = new ColumnImpl();
            String[] b = colArr[i].cell.toArray(new String[colArr[i].cell.size()]);
            a[i].name = colArr[i].name;
            if (lineCount > b.length) {
                lineCount = b.length;
            }
            for (int j = 0; j < lineCount; j++) {
                a[i].cell.add(b[j]);
            }
        }
        TableImpl t = new TableImpl();
        t.name = this.name;
        for (int i = 0; i < a.length; i++) {
            t.tablecol.add(a[i]);
        }
        return t;
    }

    @Override
    public Table tail() {
        int lineCount = 5;
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        ColumnImpl[] a = new ColumnImpl[colArr.length];
        for (int i = 0; i < colArr.length; i++) {
            a[i] = new ColumnImpl();
            String[] b = colArr[i].cell.toArray(new String[colArr[i].cell.size()]);
            a[i].name = colArr[i].name;
            if (lineCount > b.length) {
                lineCount = b.length;
            }
            for (int j = b.length - lineCount; j < b.length; j++) {
                a[i].cell.add(b[j]);
            }
        }
        TableImpl t = new TableImpl();
        t.name = this.name;
        for (int i = 0; i < a.length; i++) {
            t.tablecol.add(a[i]);
        }
        return t;
    }

    @Override
    public Table tail(int lineCount) {
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        ColumnImpl[] a = new ColumnImpl[colArr.length];
        for (int i = 0; i < colArr.length; i++) {
            a[i] = new ColumnImpl();
            String[] b = colArr[i].cell.toArray(new String[colArr[i].cell.size()]);
            a[i].name = colArr[i].name;
            if (lineCount > b.length) {
                lineCount = b.length;
            }
            for (int j = b.length - lineCount; j < b.length; j++) {
                a[i].cell.add(b[j]);
            }
        }
        TableImpl t = new TableImpl();
        t.name = this.name;
        for (int i = 0; i < a.length; i++) {
            t.tablecol.add(a[i]);
        }
        return t;
    }

    @Override
    public Table selectRows(int beginIndex, int endIndex) {
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        ColumnImpl[] a = new ColumnImpl[colArr.length];
        for (int i = 0; i < colArr.length; i++) {
            a[i] = new ColumnImpl();
            String[] b = colArr[i].cell.toArray(new String[colArr[i].cell.size()]);
            a[i].name = colArr[i].name;
            for (int j = beginIndex; j < endIndex; j++) {
                a[i].cell.add(b[j]);
            }
        }
        TableImpl t = new TableImpl();
        t.name = this.name;
        for (int i = 0; i < a.length; i++) {
            t.tablecol.add(a[i]);
        }
        return t;
    }

    @Override
    public Table selectRowsAt(int... indices) {
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        ColumnImpl[] a = new ColumnImpl[colArr.length];
        for (int i = 0; i < colArr.length; i++) {
            a[i] = new ColumnImpl();
            String[] b = colArr[i].cell.toArray(new String[colArr[i].cell.size()]);
            a[i].name = colArr[i].name;
            for (int j : indices) {
                a[i].cell.add(b[j]);
            }
        }
        TableImpl t = new TableImpl();
        t.name = this.name;
        for (int i = 0; i < a.length; i++) {
            t.tablecol.add(a[i]);
        }
        return t;
    }

    @Override
    public Table selectColumns(int beginIndex, int endIndex) {
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        ColumnImpl[] a = new ColumnImpl[colArr.length];
        for (int i = beginIndex; i < endIndex; i++) {
            a[i] = new ColumnImpl();
            String[] b = colArr[i].cell.toArray(new String[colArr[i].cell.size()]);
            a[i].name = colArr[i].name;
            for (int j = 0; j < b.length; j++) {
                a[i].cell.add(b[j]);
            }
        }
        TableImpl t = new TableImpl();
        t.name = this.name;
        for (int i = 0; i < endIndex - beginIndex; i++) {
            t.tablecol.add(a[i]);
        }
        return t;
    }

    @Override
    public Table selectColumnsAt(int... indices) {
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        ColumnImpl[] a = new ColumnImpl[colArr.length];
        for (int i : indices) {
            a[i] = new ColumnImpl();
            String[] b = colArr[i].cell.toArray(new String[colArr[i].cell.size()]);
            a[i].name = colArr[i].name;
            for (int j = 0; j < b.length; j++) {
                a[i].cell.add(b[j]);
            }
        }
        TableImpl t = new TableImpl();
        t.name = this.name;
        for (int i : indices) {
            t.tablecol.add(a[i]);
        }
        return t;
    }

    @Override
    public <T> Table selectRowsBy(String columnName, Predicate<T> predicate) {
        return null;
    }

    @Override
    public Table sort(int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        int tableLength = tablecol.size();
        int cellLength = 0;
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tableLength]);
        //리스트 전부 제거 후 정렬하고 다시 추가
        tablecol.clear();
        int cl = colArr[0].cell.size();
        String[][] temp = new String[colArr[0].cell.size()][tableLength];
        //배열에 문자열 삽입, column 길이 구하기
        for (int j = 0; j < colArr.length; j++) {
            ColumnImpl tmp = colArr[j];
            String[] arr = tmp.cell.toArray(new String[tmp.cell.size()]);
            for (int k = 0; k < arr.length; k++) {
                temp[k][j] = arr[k];
            }
            colArr[j].cell.clear();
            cellLength = arr.length;
        }
        int nullCount = 0;
        int iA = isAscending ? 1 : -1;
        if (isNullFirst) {
            for (int i = 0; i < cellLength; i++) {
                if (temp[i][byIndexOfColumn] == null) {
                    String[] t = temp[i];
                    temp[i] = temp[nullCount];
                    temp[nullCount] = t;
                    nullCount++;
                }
            }
            for (int i = nullCount; i < cellLength; i++) {
                for (int j = i + 1; j < cellLength; j++) {
                    if (iA * temp[i][byIndexOfColumn].compareTo(temp[j][byIndexOfColumn]) > 0) {
                        String[] t = temp[i];
                        temp[i] = temp[j];
                        temp[j] = t;
                    }
                }
            }
        } else {
            for (int i = cellLength - 1; i >= 0; i--) {
                if (temp[i][byIndexOfColumn] == null) {
                    String[] t = temp[i];
                    temp[i] = temp[cellLength - nullCount - 1];
                    temp[cellLength - nullCount - 1] = t;
                    nullCount++;
                }
            }
            for (int i = 0; i < cellLength - nullCount; i++) {
                for (int j = i + 1; j < cellLength - nullCount; j++) {
                    if (iA * temp[i][byIndexOfColumn].compareTo(temp[j][byIndexOfColumn]) > 0) {
                        String[] t = temp[i];
                        temp[i] = temp[j];
                        temp[j] = t;
                    }
                }
            }
        }

        for (int i = 0; i < tableLength; i++) {
            for (int j = 0; j < cl; j++) {
                colArr[i].cell.add(temp[j][i]);
            }
        }
        for (int i = 0; i < tableLength; i++) {
            tablecol.add(colArr[i]);
        }
        return this;
    }

    @Override
    public int getRowCount() {
        return tablecol.size();
    }

    @Override
    public int getColumnCount() {
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        return colArr[0].count();
    }

    @Override
    public Column getColumn(int index) {
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        return colArr[index];
    }

    @Override
    public Column getColumn(String name) {
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        for (int i = 0; i < colArr.length; i++) {
            if (colArr[i].name == name) {
                return colArr[i];
            }
        }
        return null;
    }
}


package database;

import java.util.*;
import java.util.function.Predicate;

class TableImpl implements Table {

    String name;
    List<ColumnImpl> tablecol = new ArrayList<>();

    @Override
    public Table crossJoin(Table rightTable) {
        TableImpl t = new TableImpl();
        t.name = this.name+"+"+rightTable.getName();
        ColumnImpl [] c = new ColumnImpl[this.getRowCount()+rightTable.getRowCount()];
        ColumnImpl[] thisColArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        for(int i=0;i<this.getRowCount();i++){
            c[i] = new ColumnImpl();
            c[i].name=this.name+"."+thisColArr[i].name;
            String [] thisCellArr = thisColArr[i].cell.toArray(new String[thisColArr[i].cell.size()]);
            for(int j=0;j<thisCellArr.length;j++){
                for(int k=0;k<rightTable.getColumnCount();k++) {
                    c[i].cell.add(thisCellArr[j]);
                }
            }
        }
        ColumnImpl[] rightTableColArr = new ColumnImpl[rightTable.getRowCount()];
        for(int i=this.getRowCount();i<c.length;i++) {
            c[i] = new ColumnImpl();
            rightTableColArr[i - this.getRowCount()] = (ColumnImpl) rightTable.getColumn(i - this.getRowCount());
            c[i].name = rightTable.getName() + "." + rightTableColArr[i - this.getRowCount()].name;
            String[] rightCellArr = rightTableColArr[i - this.getRowCount()].cell.toArray(new String[rightTableColArr[i - this.getRowCount()].cell.size()]);
            for(int k=0;k<c[0].cell.size()/rightTable.getColumnCount();k++) {
                for (int j = 0; j < rightTable.getColumnCount(); j++) {
                    c[i].cell.add(rightCellArr[j]);
                }
            }
        }
        for(int i=0;i<c.length;i++){
            t.tablecol.add(c[i]);
        }
        return t;
    }

    @Override
    public Table innerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        TableImpl t=(TableImpl) this.crossJoin(rightTable);
        JoinColumn [] joinColumnsArr = joinColumns.toArray(new JoinColumn[joinColumns.size()]);
        String[][] temp = new String[t.getRowCount()][t.getColumnCount()];
        for(int i=0;i<joinColumnsArr.length;i++){
            String a = this.name+"."+joinColumnsArr[i].getColumnOfThisTable();
            String b = rightTable.getName()+"."+joinColumnsArr[i].getColumnOfAnotherTable();
            ColumnImpl A = (ColumnImpl) t.getColumn(a);
            ColumnImpl B = (ColumnImpl) t.getColumn(b);
            String [] aa = A.cell.toArray(new String[A.cell.size()]);
            String [] bb = B.cell.toArray(new String[B.cell.size()]);
            List<Integer> I = new ArrayList<>();
            for(int k=0;k<aa.length;k++){
                if(aa[k].equals(bb[k])){
                    I.add(k);
                }
            }
            int[] arr = new int[I.size()];
            for (int k = 0 ; k < I.size() ; k++) {
                arr[k] = I.get(k).intValue();
            }
            t=(TableImpl) t.selectRowsAt(arr);
        }
        return t;
    }

    @Override
    public Table outerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        int a1=this.getRowCount();
        int a3=this.getColumnCount();
        int a2=rightTable.getRowCount();
        int a4=rightTable.getColumnCount();
        TableImpl t = new TableImpl();
        JoinColumn [] joinColumnsArr = joinColumns.toArray(new JoinColumn[joinColumns.size()]);
        for(int Q=0;Q<joinColumnsArr.length;Q++) {
            ColumnImpl A = (ColumnImpl) this.getColumn(joinColumnsArr[Q].getColumnOfThisTable());
            ColumnImpl B = (ColumnImpl) rightTable.getColumn(joinColumnsArr[Q].getColumnOfAnotherTable());
            int aa = 0, bb = 0;
            t.name = this.name + "+" + rightTable.getName();
            ColumnImpl[] c = new ColumnImpl[a1 + a2];
            ColumnImpl[] thisColArr = tablecol.toArray(new ColumnImpl[a1]);
            String[][] s = new String[a3][a1 + a2];
            for (int i = 0; i < a1; i++) {
                c[i] = new ColumnImpl();
                c[i].name = this.name + "." + thisColArr[i].name;
                if (thisColArr[i].name.equals(A.name)) {
                    aa = i;
                }
                String[] thisCellArr = thisColArr[i].cell.toArray(new String[thisColArr[i].cell.size()]);
                for (int j = 0; j < a3; j++) {
                    s[j][i] = thisCellArr[j];
                }
            }
            ColumnImpl[] rightTableColArr = new ColumnImpl[a2];
            for (int i = a1; i < a1 + a2; i++) {
                c[i] = new ColumnImpl();
                rightTableColArr[i - a1] = (ColumnImpl) rightTable.getColumn(i - a1);
            }
            String[][] rs = new String[a4][a2];
            for (int i = 0; i < a2; i++) {
                String[] tmp = rightTableColArr[i].cell.toArray(new String[rightTableColArr[i].cell.size()]);
                for (int j = 0; j < a4; j++) {
                    rs[j][i] = tmp[j];
                }
            }
            for (int i = a1; i < a1 + a2; i++) {
                c[i] = new ColumnImpl();
                c[i].name = rightTable.getName() + "." + rightTableColArr[i - a1].name;
                if (rightTableColArr[i - a1].name.equals(B.name)) {
                    bb = i;
                }
            }
            for (int i = 0; i < a3; i++) {
                for (int j = 0; j < a4; j++) {
                    if (rs[j][bb - a1].equals(s[i][aa])) {
                        for (int k = a1; k < a1 + a2; k++) {
                            s[i][k] = rs[j][k - a1];
                        }
                    }
                }
            }
            int cnt = 0;
            for (int i = a3 - 1; i >= 0; i--) {
                if (s[i][a1] == null) {
                    String[] tmp = s[i];
                    s[i] = s[a3 - 1 - cnt];
                    s[a3 - 1 - cnt] = tmp;
                    cnt++;
                }
            }
            for (int i = 0; i < a3 - cnt; i++) {
                for (int j = i + 1; j < a3 - cnt; j++) {
                    if (s[i][0].compareTo(s[j][0]) > 0) {
                        String[] tmp = s[i];
                        s[i] = s[j];
                        s[j] = tmp;
                    }
                }
            }
            for (int i = 0; i < a1 + a2; i++) {
                for (int j = 0; j < a3; j++) {
                    c[i].cell.add(s[j][i]);
                }
            }
            for (int i = 0; i < a1 + a2; i++) {
                t.tablecol.add(c[i]);
            }
        }
        return t;
    }

    @Override
    public Table fullOuterJoin(Table rightTable, List<JoinColumn> joinColumns) {
        JoinColumn [] joinColumnsArr = joinColumns.toArray(new JoinColumn[joinColumns.size()]);
        TableImpl t = (TableImpl) this.outerJoin(rightTable,joinColumns);
        for(int Q=0;Q<joinColumnsArr.length;Q++) {
            int bb = 0, aa = 0;
            ColumnImpl[] c = t.tablecol.toArray(new ColumnImpl[t.tablecol.size()]);
            for (int i = 0; i < c.length; i++) {
                if (c[i].name.equals(this.getName() + "." + joinColumnsArr[Q].getColumnOfThisTable())) {
                    aa = i;
                }
            }
            String[] s = c[aa].cell.toArray(new String[t.getColumnCount()]);
            ColumnImpl[] r = new ColumnImpl[rightTable.getRowCount()];
            String[][] rA = new String[rightTable.getColumnCount()][r.length];
            for (int i = 0; i < r.length; i++) {
                r[i] = (ColumnImpl) rightTable.getColumn(i);
                if (r[i].name.equals(joinColumnsArr[Q].getColumnOfAnotherTable())) {
                    bb = i;
                }
                String[] tmp = r[i].cell.toArray(new String[r[i].cell.size()]);
                for (int j = 0; j < tmp.length; j++) {
                    rA[j][i] = tmp[j];
                }
            }
            for (int i = 0; i < rA.length; i++) {
                int cnt = 0;
                for (int j = 0; j < s.length; j++) {
                    if (!(rA[i][bb].equals(s[j]))) {
                        cnt++;
                    }
                }
                if (cnt == s.length) {
                    for (int k = 0; k < this.getRowCount(); k++) {
                        c[k].cell.add(null);
                    }
                    for (int k = this.getRowCount(); k < t.getRowCount(); k++) {
                        c[k].cell.add(rA[i][k - this.getRowCount()]);
                    }
                }
            }
            t.tablecol.clear();
            for (int i = 0; i < c.length; i++) {
                t.tablecol.add(c[i]);
            }
        }
        return t;
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
                System.out.printf(" %" + columnsMaxLength[j] + "s |", temp[j][i]);
            }
            System.out.print("\n");
        }
    }

    @Override
    public boolean equals(Object obj) {
        TableImpl p = (TableImpl) obj;
        if(p.name.equals(name)) return true;
        else return false;
    }
    @Override
    public int hashCode() {
        return Objects.hash(name);
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
            int ss=0;
            for(int j=0;j<arr.length;j++) {
                if (arr[j]!=null&&!arr[j].matches("[+-]?\\d*(\\.\\d+)?")) {
                    ss++;
                }
            }
            if(ss!=0){
                tmp[i + 1][3] = "String";
                S++;
            }
            else{
                tmp[i + 1][3] = "int";
                I++;
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
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        ColumnImpl c =  new ColumnImpl();
        for(int i=0;i<tablecol.size();i++) {
            if(colArr[i].name.equals(columnName)) {
                c = colArr[i];
            }
        }
        String [] s = c.cell.toArray(new String[c.count()]);
        List<Integer> arr = new ArrayList<>();
        for(int i=0;i<s.length;i++){
            if(s[i]!=null&&s[i].matches("[+-]?\\d*(\\.\\d+)?")){
                Integer j = Integer.parseInt(s[i]);
                if(predicate.test( (T) j )){
                    arr.add(i);
                }
            }
            else{
                if(predicate.test((T)s[i])){
                    arr.add(i);
                }
            }
        }
        int [] ar = new int[arr.size()];
        for(int i=0;i<arr.size();i++){
            ar[i] = arr.get(i).intValue();
        }
        Table t = this.selectRowsAt(ar);
        return t;
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
        return tablecol.get(0).count();
    }

    @Override
    public Column getColumn(int index) {
        return tablecol.get(index);
    }

    @Override
    public Column getColumn(String name) {
        int k=0;
        ColumnImpl[] colArr = tablecol.toArray(new ColumnImpl[tablecol.size()]);
        for (int i = 0; i < colArr.length; i++) {
            if (colArr[i].name.equals(name)) {
                k=i;
            }
        }
        return tablecol.get(k);
    }
}
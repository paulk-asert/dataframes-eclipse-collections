/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.nio.file.Paths
import java.time.LocalDate

var file = getClass().classLoader.getResource("donut_orders.csv").toURI()

// roll our own CSV parsing but we could use Commons CSV, OpenCSV, ...
var lines  = Paths.get(file).readLines()
var headers = lines.head().split(',')
var orderMaps = lines.tail().collect{ it.split(',')*.trim() }.collect{
    [(headers[0]): it[0][1..-2],
     (headers[1]): it[1] as long,
     (headers[2]): it[2] as double,
     (headers[3]): LocalDate.parse(it[3], 'yyyy-MM-dd')]
}

var orders = GQ {
    from o in orderMaps
    select o.Customer, o.Count, o.Price, o.Date
}.toList()

println GQ {
    from o in orders
    select sum(o['Price']) as TotalPrice, sum(o['Count']) as TotalCount
}
/*
+------------+------------+
| TotalPrice | TotalCount |
+------------+------------+
| 83.29      | 19         |
+------------+------------+
*/

println GQ {
    from o in orders
    where o.Count >= 5
    select o
}
/*
+-----------+-------+-------+------------+
| Customer  | Count | Price | Date       |
+-----------+-------+-------+------------+
| Archibald | 5     | 23.45 | 2020-10-15 |
| Bridget   | 10    | 40.34 | 2020-11-10 |
+-----------+-------+-------+------------+
*/

println GQ {
    from o in orders
    select o.Customer, o.Count, o.Price, o.Date, o.Price / o.Count as AvgDonutPrice
}
/*
+-----------+-------+-------+------------+--------------------+
| Customer  | Count | Price | Date       | AvgDonutPrice      |
+-----------+-------+-------+------------+--------------------+
| Archibald | 5     | 23.45 | 2020-10-15 | 4.6899999999999995 |
| Bridget   | 10    | 40.34 | 2020-11-10 | 4.034000000000001  |
| Clyde     | 4     | 19.5  | 2020-10-19 | 4.875              |
+-----------+-------+-------+------------+--------------------+
*/

println GQ {
    from o in orders
    orderby o.Date
    select o
}
/*
+-----------+-------+-------+------------+
| Customer  | Count | Price | Date       |
+-----------+-------+-------+------------+
| Archibald | 5     | 23.45 | 2020-10-15 |
| Clyde     | 4     | 19.5  | 2020-10-19 |
| Bridget   | 10    | 40.34 | 2020-11-10 |
+-----------+-------+-------+------------+
*/

println GQ {
    from o in orders + [Customer: 'Eve', Count: 2L, Price: 9.8, Date: LocalDate.of(2020, 12, 5)]
    select o
}
/*
+-----------+-------+-------+------------+
| Customer  | Count | Price | Date       |
+-----------+-------+-------+------------+
| Archibald | 5     | 23.45 | 2020-10-15 |
| Bridget   | 10    | 40.34 | 2020-11-10 |
| Clyde     | 4     | 19.5  | 2020-10-19 |
| Eve       | 2     | 9.8   | 2020-12-05 |
+-----------+-------+-------+------------+
*/

println GQ {
    from o in orders
    select max(o['Price']) as MaxPrice, min(o['Price']) as MinPrice, sum(o['Price']) as Total
}
/*
+----------+----------+-------+
| MaxPrice | MinPrice | Total |
+----------+----------+-------+
| 40.34    | 19.5     | 83.29 |
+----------+----------+-------+
*/

var joining1 = [
        [Foo: 'Pinky', Bar: 'pink', Letter: 'B', Baz: 8],
        [Foo: 'Inky', Bar: 'cyan', Letter: 'C', Baz: 9],
        [Foo: 'Clyde', Bar: 'orange', Letter: 'D', Baz: 10]
]
var joining2 = [
        [Name: 'Grapefruit', Color: 'pink', Code: 'B', Number: 2],
        [Name: 'Orange', Color: 'orange', Code: 'D', Number: 4],
        [Name: 'Apple', Color: 'red', Code: 'A', Number: 1]
]
println GQ {
    from a in joining1
    fulljoin b in joining2 on a.Bar == b.Color && a.Letter == b.Code
    select a?.Foo as Foo, (a?.Bar ?: b.Color) as Bar, (a?.Letter ?: b?.Code) as Letter, a?.Baz as Baz, b?.Name as Name, b?.Number as Number
}
/*
+-------+--------+--------+-----+------------+--------+
| Foo   | Bar    | Letter | Baz | Name       | Number |
+-------+--------+--------+-----+------------+--------+
| Pinky | pink   | B      | 8   | Grapefruit | 2      |
| Inky  | cyan   | C      | 9   |            |        |
| Clyde | orange | D      | 10  | Orange     | 4      |
|       | red    | A      |     | Apple      | 1      |
+-------+--------+--------+-----+------------+--------+
*/

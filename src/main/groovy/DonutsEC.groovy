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

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame
import io.github.vmzakharov.ecdataframe.dataset.CsvDataSet
import io.github.vmzakharov.ecdataframe.dataset.CsvSchema
import org.eclipse.collections.api.factory.Lists

import java.nio.file.Paths
import java.time.LocalDate

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.*
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.*

var file = getClass().classLoader.getResource("donut_orders.csv").toURI()
var donutSchema = new CsvSchema().tap {
    addColumn("Customer", STRING)
    addColumn("Count", LONG)
    addColumn("Price", DOUBLE)
    addColumn("Date", DATE)
}
var orders  = new CsvDataSet(Paths.get(file), "Donut Orders", donutSchema).loadAsDataFrame()

println orders.sum(Lists.immutable.of("Count", "Price")).asCsvString()
/*
Count,Price
19,83.29
*/

println orders.selectBy("Count >= 5").asCsvString()
/*
Customer,Count,Price,Date
"Archibald",5,23.45,2020-10-15
"Bridget",10,40.34,2020-11-10
*/

orders.addDoubleColumn("AvgDonutPrice", "Price / Count")
println orders.asCsvString()
/*
Customer,Count,Price,Date,AvgDonutPrice
"Archibald",5,23.45,2020-10-15,4.6899999999999995
"Bridget",10,40.34,2020-11-10,4.034000000000001
"Clyde",4,19.5,2020-10-19,4.875
*/

orders.dropColumn("AvgDonutPrice")
println orders.sortBy(Lists.immutable.of("Date")).asCsvString()
/*
Customer,Count,Price,Date
"Archibald",5,23.45,2020-10-15
"Clyde",4,19.5,2020-10-19
"Bridget",10,40.34,2020-11-10
*/

var otherOrders = new DataFrame("Other Donut Orders")
        .addStringColumn("Customer")
        .addLongColumn("Count")
        .addDoubleColumn("Price")
        .addDateColumn("Date")
        .addRow("Eve",  2, 9.80, LocalDate.of(2020, 12, 5))
println orders.union(otherOrders).asCsvString()
/*
Customer,Count,Price,Date
"Archibald",5,23.45,2020-10-15
"Bridget",10,40.34,2020-11-10
"Clyde",4,19.5,2020-10-19
"Eve",2,9.8,2020-12-05
*/

println orders.aggregate(Lists.immutable.of(
        max("Price", "MaxPrice"),
        min("Price", "MinPrice"),
        sum("Price", "Total"))).asCsvString()
/*
MaxPrice,MinPrice,Total
40.34,19.5,83.29
*/

var joining1 = new DataFrame("df1")
        .addStringColumn("Foo").addStringColumn("Bar").addStringColumn("Letter").addLongColumn("Baz")
        .addRow("Pinky", "pink", "B", 8)
        .addRow("Inky", "cyan", "C", 9)
        .addRow("Clyde", "orange", "D", 10)

var joining2 = new DataFrame("df2")
        .addStringColumn("Name").addStringColumn("Color").addStringColumn("Code").addLongColumn("Number")
        .addRow("Grapefruit", "pink", "B", 2)
        .addRow("Orange", "orange", "D", 4)
        .addRow("Apple", "red", "A", 1)

println joining1.outerJoin(joining2, Lists.immutable.of("Bar", "Letter"), Lists.immutable.of("Color", "Code")).asCsvString()

/*
Foo,Bar,Letter,Baz,Name,Number
"Inky","cyan","C",9,,
"Clyde","orange","D",10,"Orange",4
"Pinky","pink","B",8,"Grapefruit",2
,"red","A",,"Apple",1
 */
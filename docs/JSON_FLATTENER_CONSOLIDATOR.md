# JsonFlattenerConsolidator

## 🎯 What Does It Do?

Imagine you have a complex, multi-story JSON mansion with rooms within rooms, and arrays of furniture in each room. The `JsonFlattenerConsolidator` is like a skilled architect who can:

1. **Flatten** the mansion into a single-floor blueprint (making nested JSON flat)
2. **Consolidate** similar furniture into organized lists (combining array elements)
3. **Explode** specific rooms into separate floor plans (array explosion for data normalization)

This tool transforms deeply nested, complex JSON structures into flat, analyzable formats perfect for data processing systems like Apache Spark, databases, or analytics platforms.

## 🏗️ Core Concepts

### 1. Flattening: From Nested to Flat

Think of nested JSON like a Russian matryoshka doll - dolls within dolls. Flattening unpacks all these dolls and lays them out side by side.

```json
// Before (Nested)
{
  "user": {
    "profile": {
      "name": "Alice",
      "settings": {
        "theme": "dark"
      }
    }
  }
}

// After (Flattened)
{
  "user_profile_name": "Alice",
  "user_profile_settings_theme": "dark"
}
```

### 2. Consolidation: Array Management

When you have arrays, consolidation is like organizing a messy drawer - similar items get grouped together:

```json
// Before
{
  "orders": [
    {"id": "A1", "amount": 100},
    {"id": "A2", "amount": 200}
  ]
}

// After (Consolidated)
{
  "orders_id": "A1,A2",
  "orders_amount": "100,200",
  "orders_id_count": 2,
  "orders_amount_avg": 150.0
}
```

### 3. Explosion: Array Normalization

Sometimes you need the opposite of consolidation - explosion creates separate records for each array element, like dealing cards from a deck:

```json
// Before
{
  "orderId": "123",
  "items": ["Widget", "Gadget", "Tool"]
}

// After (Exploded into 3 records)
[
  {"orderId": "123", "items": "Widget", "items_explosion_index": 0},
  {"orderId": "123", "items": "Gadget", "items_explosion_index": 1},
  {"orderId": "123", "items": "Tool", "items_explosion_index": 2}
]
```

## 🚀 Quick Start

### Basic Usage

```java
// Create a flattener with default settings
JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
    ",",      // delimiter for array values
    "null",   // placeholder for null values
    50,       // max nesting depth
    1000,     // max array size
    false     // don't include array indices in values
);

// Flatten and consolidate
String jsonInput = "{\"users\": [{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]}";
String flattened = flattener.flattenAndConsolidateJson(jsonInput);
// Result: {"users_name": "Alice,Bob", "users_name_count": 2, ...}
```

### Array Explosion

```java
// Create a flattener with explosion paths
JsonFlattenerConsolidator exploder = new JsonFlattenerConsolidator(
    ",", "null", 50, 1000, false, true,
    "orders.items"  // Explode the items array within orders
);

String jsonInput = """
{
  "orderId": "123",
  "orders": [{
    "items": ["A", "B"],
    "customer": "Alice"
  }]
}
""";

List<String> exploded = exploder.flattenAndExplodeJson(jsonInput);
// Creates 2 records, one for each item
```

## ⚙️ Configuration Options

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `arrayDelimiter` | String | "," | Character(s) used to join array values |
| `nullPlaceholder` | String | null | String to represent null values |
| `maxNestingDepth` | int | 50 | Maximum depth before converting to string |
| `maxArraySize` | int | 1000 | Maximum array elements to process |
| `consolidateWithMatrixDenotorsInValue` | boolean | false | Include array indices in consolidated values |
| `gatherStatistics` | boolean | true | Generate statistics for arrays (count, avg, etc.) |
| `explosionPaths` | String... | none | Paths to explode instead of consolidate |

### Configuration Examples

#### Memory-Conscious Configuration
```java
// For large datasets with deep nesting
JsonFlattenerConsolidator memoryFriendly = new JsonFlattenerConsolidator(
    ",",    // delimiter
    null,   // no null placeholder (saves memory)
    20,     // reduced nesting depth
    100,    // smaller array limit
    false,  // no matrix denotors
    false   // no statistics (saves memory)
);
```

#### Analytics-Focused Configuration
```java
// For data analysis with full statistics
JsonFlattenerConsolidator analytics = new JsonFlattenerConsolidator(
    "|",    // pipe delimiter for easier parsing
    "NULL", // explicit null marker
    50,     // standard depth
    1000,   // full array processing
    true,   // include indices for matrix operations
    true    // gather all statistics
);
```

#### Explosion Configuration
```java
// For normalizing nested e-commerce data
JsonFlattenerConsolidator normalizer = new JsonFlattenerConsolidator(
    ",", "null", 50, 1000, false, true,
    "orders.lineItems",           // Explode line items
    "orders.lineItems.discounts"  // Further explode discounts
);
```

## 🎮 How It Works

### The Journey of JSON Transformation

1. **Parsing Phase**: Your JSON enters like a guest at a hotel, and we validate its credentials (proper JSON format).

2. **Flattening Phase**: Like a hotel concierge organizing luggage:
    - Objects become dot-separated paths: `guest.room.number` → `guest_room_number`
    - Arrays get special treatment based on configuration
    - We use a queue (not recursion) to avoid stack overflow - think of it as taking the elevator instead of climbing infinite stairs

3. **Consolidation Phase**: Like a hotel housekeeper organizing items:
    - Similar items (array elements) get grouped
    - Statistics are calculated (how many towels, average size, etc.)
    - Field names get underscores instead of dots for database compatibility

4. **Explosion Phase** (if configured): Like a hotel manager creating individual bills:
    - Each array element becomes a separate record
    - Parent information is preserved (which room, which guest)
    - Cartesian products for multiple explosion paths

### The Magic Behind Arrays

Arrays are the trickiest guests in our JSON hotel. Here's how we handle them:

- **Primitive Arrays** (numbers, strings): Combined into comma-separated strings
- **Object Arrays**: Each object's fields are processed separately, then consolidated
- **Nested Arrays**: Handled recursively up to `maxNestingDepth`
- **Explosion Mode**: Arrays marked for explosion maintain their individual elements

## 📊 Understanding the Output

### Standard Flattening Output
```json
{
  "user_name": "Alice",
  "user_orders_id": "A1,A2,A3",
  "user_orders_amount": "100,200,150",
  "user_orders_id_count": 3,
  "user_orders_id_distinct_count": 3,
  "user_orders_amount_avg": 150.0,
  "user_orders_amount_min_length": 3,
  "user_orders_amount_max_length": 3,
  "user_orders_amount_type": "numeric_list_consolidated"
}
```

### Explosion Output
```json
[
  {
    "user_name": "Alice",
    "user_orders_id": "A1",
    "user_orders_amount": "100",
    "user_orders_explosion_index": 0
  },
  {
    "user_name": "Alice", 
    "user_orders_id": "A2",
    "user_orders_amount": "200",
    "user_orders_explosion_index": 1
  }
]
```

## 🎯 Use Cases

### 1. Data Lake Ingestion
Transform varied JSON from APIs into consistent schemas for your data lake:

```java
// Configure for data lake
JsonFlattenerConsolidator lakeFriendly = new JsonFlattenerConsolidator(
    ",", "null", 30, 500, false, true
);

// Process API responses
String apiResponse = getApiResponse();
String flattenedForLake = lakeFriendly.flattenAndConsolidateJson(apiResponse);
// Now ready for Parquet/ORC storage
```

### 2. Spark DataFrame Creation
Prepare JSON for Spark SQL analysis:

```java
// Explode arrays for proper DataFrame structure
JsonFlattenerConsolidator sparkFriendly = new JsonFlattenerConsolidator(
    ",", null, 50, 1000, false, false,
    "transactions.items"  // Normalize transaction items
);

List<String> normalizedRecords = sparkFriendly.flattenAndExplodeJson(complexJson);
// Each record is now a flat JSON ready for Spark
```

### 3. Database Migration
Convert document-store data for relational databases:

```java
// Configure for SQL compatibility
JsonFlattenerConsolidator sqlFriendly = new JsonFlattenerConsolidator(
    "|", "NULL", 20, 100, false, true
);

String documentJson = getMongoDocument();
String relationalFormat = sqlFriendly.flattenAndConsolidateJson(documentJson);
// Field names are SQL-safe with underscores
```

## ⚠️ Important Considerations

### Performance
- **Time Complexity**: O(n) where n is the total number of nodes
- **Space Complexity**: O(d) where d is the maximum depth
- Large arrays are truncated at `maxArraySize` for safety
- Explosion creates multiple records - beware of Cartesian products!

### Memory Safety
- Uses iterative approach (no stack overflow)
- Configurable limits prevent memory exhaustion
- Statistics gathering can be disabled for better performance

### Data Integrity
- Original data types preserved during flattening
- Unicode and special characters handled correctly
- Null values consistently represented
- Array order maintained in consolidation

## 🐛 Common Pitfalls and Solutions

### Pitfall 1: Cartesian Explosion
```java
// DON'T: This creates 1000 records!
new JsonFlattenerConsolidator(..., "users", "products", "regions");
// 10 users × 10 products × 10 regions = 1000 records

// DO: Explode one path at a time or carefully plan combinations
new JsonFlattenerConsolidator(..., "orders.items");
```

### Pitfall 2: Deep Nesting Performance
```java
// DON'T: Process extremely deep JSON without limits
new JsonFlattenerConsolidator(",", null, 1000, 10000, false);

// DO: Set reasonable limits
new JsonFlattenerConsolidator(",", null, 30, 1000, false);
```

### Pitfall 3: Special Characters in Data
```java
// DON'T: Use delimiter that appears in your data
new JsonFlattenerConsolidator(",", null, ...); // If data contains commas

// DO: Choose unique delimiter
new JsonFlattenerConsolidator("|~|", null, ...); // Unlikely to appear in data
```

## 🔧 Advanced Features

### Matrix Denotors
When `consolidateWithMatrixDenotorsInValue` is true, array indices are preserved in values:

```json
// Input: {"matrix": [[1,2], [3,4]]}
// Output with matrix denotors: 
{
  "matrix": "[0][0]1,[0][1]2,[1][0]3,[1][1]4"
}
```

### Custom Statistics
Statistics help understand your data:
- `_count`: Number of elements
- `_distinct_count`: Unique elements
- `_min_length`, `_max_length`: String length bounds
- `_avg_length`: Average string length
- `_type`: Data type classification

### Selective Explosion
Choose exactly which arrays to explode:

```java
// Only explode specific paths
JsonFlattenerConsolidator selective = new JsonFlattenerConsolidator(
    ",", null, 50, 1000, false, true,
    "orders.items",              // Explode items
    "users.addresses.phoneNumbers" // And phone numbers
    // But NOT "users.orders" - this stays consolidated
);
```

## 📈 Performance Tuning

### For High Volume
```java
JsonFlattenerConsolidator highVolume = new JsonFlattenerConsolidator(
    ",",    // Simple delimiter
    null,   // No null placeholder
    25,     // Moderate depth
    200,    // Limited array size
    false,  // No matrix denotors
    false   // No statistics
);
```

### For Deep Analysis
```java
JsonFlattenerConsolidator deepAnalysis = new JsonFlattenerConsolidator(
    "|",    // Clear delimiter
    "NULL", // Explicit nulls
    100,    // Deep nesting support
    5000,   // Large array support
    true,   // Matrix notation
    true    // Full statistics
);
```

## 🎉 Conclusion

The `JsonFlattenerConsolidator` is your Swiss Army knife for JSON transformation. Whether you're preparing data for analytics, migrating between systems, or normalizing complex structures, it provides the flexibility and safety you need.

Remember: With great flattening power comes great responsibility. Always test with your actual data patterns and monitor performance in production!

---

*Happy Flattening! 🚀*

# TOP 10 CUSTOMERS 
db.transactions.aggregate([
  {
    $group: {
      _id: "$user_id",
      total_spent: { $sum: "$total" },
      number_of_orders: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      user_id: "$_id",
      total_spent: { $round: ["$total_spent", 2] },
      number_of_orders: 1
    }
  },
  {
    $sort: { total_spent: -1 }
  },
  {
    $limit: 5
  }
])

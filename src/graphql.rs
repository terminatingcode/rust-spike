use async_graphql::{Object, SimpleObject};
use serde::{Deserialize, Serialize};

#[derive(SimpleObject, Deserialize, Serialize)]
struct Merchant {
    id: String,
    name: String,
    founded_date: String,
    industry: String,
    num_employees: i32,
    vat_number: String,
    description: String,
}

pub struct Query;

#[Object]
impl Query {
    async fn merchants(&self) -> Vec<Merchant> {
        vec![
            Merchant {
                id: "merchant-123".to_string(),
                name: "Example Merchant".to_string(),
                founded_date: "2020-01-01".to_string(),
                industry: "Retail".to_string(),
                num_employees: 100,
                vat_number: "VAT123456".to_string(),
                description: "A sample merchant".to_string(),
            },
            Merchant {
                id: "merchant-456".to_string(),
                name: "Example Merchant 2".to_string(),
                founded_date: "2020-01-01".to_string(),
                industry: "Retail".to_string(),
                num_employees: 100,
                vat_number: "VAT123456".to_string(),
                description: "A sample merchant".to_string(),
            },
        ]
    }
}

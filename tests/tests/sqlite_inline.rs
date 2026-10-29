//! The driver suite on SQLite with `inline_connections`: operations run on the caller's
//! task instead of a task per connection.

#![cfg(feature = "sqlite")]

struct SqliteSetup;

impl SqliteSetup {
    fn new() -> Self {
        SqliteSetup
    }
}

#[async_trait::async_trait]
impl toasty_driver_integration_suite::Setup for SqliteSetup {
    fn driver(&self) -> Box<dyn toasty_core::driver::Driver> {
        Box::new(toasty_driver_sqlite::Sqlite::in_memory())
    }

    async fn delete_table(&self, _name: &str) {
        // There is no need to delete anything since the driver operates in-memory
    }

    fn configure(&self, builder: &mut toasty::db::Builder) {
        builder.inline_connections();
    }
}

// Generate all driver tests
toasty_driver_integration_suite::generate_driver_tests!(
    SqliteSetup::new(),
    native_decimal: false,
    bigdecimal_implemented: false,
    decimal_arbitrary_precision: false,
    native_timestamp: false,
    native_date: false,
    native_time: false,
    native_datetime: false,
    native_cidr: false,
    native_inet: false,
    native_macaddr: false,
    native_macaddr8: false,
    native_ilike: false,
    native_json: false,
    native_jsonb: false,
    native_array: false,
    native_enum: false,
    vec_scalar: true,
    unique_list_index: false,
    document_collections: true,
    vec_remove: false,
    vec_pop: false,
    vec_remove_at: false,
    test_connection_pool: false,
);

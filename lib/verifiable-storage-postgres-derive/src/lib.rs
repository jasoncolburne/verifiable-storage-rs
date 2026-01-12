use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Lit, parse_macro_input};

/// Derive macro for Stored - generates PostgreSQL repository implementation.
///
/// This macro supports two modes:
///
/// ## Individual Repository Mode
/// Applied to a repository struct with `item_type` and `table`, generates:
/// - `new(pool: PgPool) -> Self` constructor
/// - `VersionedRepository<T>` or `UnversionedRepository<T>` implementation
///
/// The struct must have a `pool: PgPool` field.
/// The item type must implement `Storable + Serialize + DeserializeOwned`.
///
/// Attributes:
/// - `item_type`: The type to implement the repository for (required)
/// - `table`: The table name for storage (required)
/// - `id_field`: The field name containing the SAID (default: "said")
/// - `prefix_field`: The field name containing the prefix (default: "prefix", only for versioned)
/// - `versioned`: Whether to generate VersionedRepository (default: true)
///
/// Example:
/// ```text
/// #[derive(Stored)]
/// #[stored(item_type = Domain, table = "adns_domains")]
/// pub struct DomainRepository {
///     pool: PgPool,
/// }
/// ```
///
/// ## Combined Repository Mode
/// Applied to a repository struct with `migrations`, generates:
/// - `RepositoryConnection` implementation
///
/// The struct must have sub-repository fields with `PgPool` as their first constructor arg.
///
/// Attributes:
/// - `migrations`: Path to migrations directory (required for this mode)
///
/// Example:
/// ```text
/// #[derive(Stored)]
/// #[stored(migrations = "services/adns/migrations")]
/// pub struct AdnsRepository {
///     pub domains: DomainRepository,
///     pub records: RecordRepository,
/// }
/// ```
#[proc_macro_derive(Stored, attributes(stored))]
pub fn derive_stored(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let repo_name = &input.ident;

    // Parse #[stored(...)] attribute
    let stored_attr = input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("stored"))
        .expect("No #[stored(...)] attribute found");

    // Parse the attribute arguments
    let mut item_type: Option<syn::Type> = None;
    let mut table_name: Option<String> = None;
    let mut id_field = "said".to_string();
    let mut prefix_field = "prefix".to_string();
    let mut versioned = true;
    let mut migrations: Option<String> = None;

    stored_attr
        .parse_nested_meta(|meta| {
            if meta.path.is_ident("item_type") {
                meta.input.parse::<syn::Token![=]>()?;
                item_type = Some(meta.input.parse()?);
            } else if meta.path.is_ident("table") {
                meta.input.parse::<syn::Token![=]>()?;
                let lit: Lit = meta.input.parse()?;
                if let Lit::Str(s) = lit {
                    table_name = Some(s.value());
                }
            } else if meta.path.is_ident("id_field") {
                meta.input.parse::<syn::Token![=]>()?;
                let lit: Lit = meta.input.parse()?;
                if let Lit::Str(s) = lit {
                    id_field = s.value();
                }
            } else if meta.path.is_ident("prefix_field") {
                meta.input.parse::<syn::Token![=]>()?;
                let lit: Lit = meta.input.parse()?;
                if let Lit::Str(s) = lit {
                    prefix_field = s.value();
                }
            } else if meta.path.is_ident("versioned") {
                meta.input.parse::<syn::Token![=]>()?;
                let lit: Lit = meta.input.parse()?;
                if let Lit::Bool(b) = lit {
                    versioned = b.value();
                }
            } else if meta.path.is_ident("migrations") {
                meta.input.parse::<syn::Token![=]>()?;
                let lit: Lit = meta.input.parse()?;
                if let Lit::Str(s) = lit {
                    migrations = Some(s.value());
                }
            }
            Ok(())
        })
        .expect("Failed to parse #[stored(...)] attribute");

    // Check which mode we're in
    if migrations.is_some() {
        // Combined repository mode - generate RepositoryConnection
        generate_combined_repository(repo_name, &input, migrations.as_deref())
    } else {
        // Individual repository mode - generate VersionedRepository/UnversionedRepository
        let item_type = item_type.expect("Missing item_type in #[stored(...)]");
        let table_name = table_name.expect("Missing table in #[stored(...)]");
        generate_individual_repository(
            repo_name,
            &item_type,
            &table_name,
            &id_field,
            &prefix_field,
            versioned,
        )
    }
}

fn generate_combined_repository(
    repo_name: &syn::Ident,
    input: &DeriveInput,
    migrations: Option<&str>,
) -> TokenStream {
    // Extract field names and types from the struct
    let fields = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            syn::Fields::Named(fields) => &fields.named,
            _ => panic!("Stored can only be derived for structs with named fields"),
        },
        _ => panic!("Stored can only be derived for structs"),
    };

    // Build field construction code
    let field_constructions: Vec<_> = fields
        .iter()
        .map(|f| {
            let name = f.ident.as_ref().expect("Field must have a name");
            let ty = &f.ty;
            quote! {
                #name: #ty::new(pool.clone())
            }
        })
        .collect();

    let field_names: Vec<_> = fields
        .iter()
        .map(|f| f.ident.as_ref().expect("Field must have a name"))
        .collect();

    // Get the first field name for pool access
    let first_field = field_names
        .first()
        .expect("Combined repository must have at least one field");

    // Generate the migrations path as a string literal for migrate!
    let migrations_path = migrations.unwrap_or("./migrations");

    let expanded = quote! {
        impl #repo_name {
            /// Create a new combined repository with the given pool.
            pub fn new(pool: verifiable_storage_postgres::PgPool) -> Self {
                Self {
                    #(#field_constructions),*
                }
            }

            /// Get a reference to the connection pool from the first sub-repository.
            pub fn pool(&self) -> &verifiable_storage_postgres::PgPool {
                // Access pool from first field
                &self.#first_field.pool
            }
        }

        #[async_trait::async_trait]
        impl verifiable_storage::RepositoryConnection for #repo_name {
            async fn connect(
                config: impl Into<verifiable_storage::ConnectionConfig> + Send,
            ) -> Result<Self, verifiable_storage::StorageError> {
                let config = config.into();
                let url = match config {
                    verifiable_storage::ConnectionConfig::Url(url) => url,
                };

                let pool = verifiable_storage_postgres::PgPool::connect(&url)
                    .await
                    .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;

                Ok(Self {
                    #(#field_constructions),*
                })
            }

            async fn initialize(&self) -> Result<(), verifiable_storage::StorageError> {
                let migrations_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(#migrations_path);
                verifiable_storage_postgres::Migrator::new(migrations_path)
                    .await
                    .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?
                    .run(self.pool().inner())
                    .await
                    .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;
                Ok(())
            }
        }
    };

    TokenStream::from(expanded)
}

fn generate_individual_repository(
    repo_name: &syn::Ident,
    item_type: &syn::Type,
    table_name: &str,
    id_field: &str,
    prefix_field: &str,
    versioned: bool,
) -> TokenStream {
    // Generate the new() constructor and table_name method
    let new_impl = quote! {
        impl #repo_name {
            /// The table name for this repository.
            pub const TABLE_NAME: &'static str = #table_name;

            /// Create a new repository with the given pool.
            pub fn new(pool: verifiable_storage_postgres::PgPool) -> Self {
                Self { pool }
            }
        }
    };

    let expanded = if versioned {
        quote! {
            #new_impl

            #[async_trait::async_trait]
            impl verifiable_storage::VersionedRepository<#item_type> for #repo_name {
                async fn create(
                    &self,
                    mut item: #item_type,
                ) -> Result<#item_type, verifiable_storage::StorageError> {
                    use verifiable_storage::Versioned;
                    item.derive_prefix()?;
                    self.insert(item).await
                }

                async fn update(
                    &self,
                    mut item: #item_type,
                ) -> Result<#item_type, verifiable_storage::StorageError> {
                    use verifiable_storage::Versioned;
                    item.increment()?;
                    self.insert(item).await
                }

                async fn insert(
                    &self,
                    item: #item_type,
                ) -> Result<#item_type, verifiable_storage::StorageError> {
                    verifiable_storage_postgres::bind_insert_with_table(&self.pool, &item, Self::TABLE_NAME).await?;
                    Ok(item)
                }

                async fn get_by_said(
                    &self,
                    said: &str,
                ) -> Result<Option<#item_type>, verifiable_storage::StorageError> {
                    use verifiable_storage_postgres::QueryExecutor;
                    let query = verifiable_storage_postgres::Query::<#item_type>::for_table(Self::TABLE_NAME)
                        .eq(#id_field, said)
                        .limit(1);
                    self.pool.fetch_optional(query).await
                }

                async fn get_latest(
                    &self,
                    prefix: &str,
                ) -> Result<Option<#item_type>, verifiable_storage::StorageError> {
                    use verifiable_storage_postgres::QueryExecutor;
                    let query = verifiable_storage_postgres::Query::<#item_type>::for_table(Self::TABLE_NAME)
                        .eq(#prefix_field, prefix)
                        .order_by("version", verifiable_storage_postgres::Order::Desc)
                        .limit(1);
                    self.pool.fetch_optional(query).await
                }

                async fn get_history(
                    &self,
                    prefix: &str,
                ) -> Result<Vec<#item_type>, verifiable_storage::StorageError> {
                    use verifiable_storage_postgres::QueryExecutor;
                    let query = verifiable_storage_postgres::Query::<#item_type>::for_table(Self::TABLE_NAME)
                        .eq(#prefix_field, prefix)
                        .order_by("version", verifiable_storage_postgres::Order::Asc);
                    self.pool.fetch(query).await
                }

                async fn exists(
                    &self,
                    prefix: &str,
                ) -> Result<bool, verifiable_storage::StorageError> {
                    use verifiable_storage_postgres::QueryExecutor;
                    let query = verifiable_storage_postgres::Query::<#item_type>::for_table(Self::TABLE_NAME)
                        .eq(#prefix_field, prefix)
                        .limit(1);
                    let result = self.pool.fetch_optional(query).await?;
                    Ok(result.is_some())
                }
            }
        }
    } else {
        quote! {
            #new_impl

            #[async_trait::async_trait]
            impl verifiable_storage::UnversionedRepository<#item_type> for #repo_name {
                async fn create(
                    &self,
                    mut item: #item_type,
                ) -> Result<#item_type, verifiable_storage::StorageError> {
                    use verifiable_storage::SelfAddressed;
                    item.derive_said()?;
                    self.insert(item).await
                }

                async fn insert(
                    &self,
                    item: #item_type,
                ) -> Result<#item_type, verifiable_storage::StorageError> {
                    verifiable_storage_postgres::bind_insert_with_table(&self.pool, &item, Self::TABLE_NAME).await?;
                    Ok(item)
                }

                async fn get_by_said(
                    &self,
                    said: &str,
                ) -> Result<Option<#item_type>, verifiable_storage::StorageError> {
                    use verifiable_storage_postgres::QueryExecutor;
                    let query = verifiable_storage_postgres::Query::<#item_type>::for_table(Self::TABLE_NAME)
                        .eq(#id_field, said)
                        .limit(1);
                    self.pool.fetch_optional(query).await
                }
            }
        }
    };

    TokenStream::from(expanded)
}

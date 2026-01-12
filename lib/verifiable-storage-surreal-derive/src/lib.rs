use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Lit, parse_macro_input};

/// Derive macro for Stored - generates SurrealDB repository implementation.
///
/// Applied to a repository struct, generates either:
/// - `impl VersionedRepository<T>` when `versioned = true` (default)
/// - `impl UnversionedRepository<T>` when `versioned = false`
///
/// Also generates a `new()` constructor that connects to SurrealDB.
///
/// The struct must have a `db: Surreal<Client>` field.
///
/// Attributes:
/// - `item_type`: The type to implement the repository for (required)
/// - `table`: The table name for storage (required)
/// - `namespace`: The SurrealDB namespace (required)
/// - `id_field`: The field name containing the SAID (default: "said")
/// - `prefix_field`: The field name containing the prefix (default: "prefix", only used when versioned)
/// - `versioned`: Whether to generate VersionedRepository (default: true)
/// - `signatures`: Whether to generate signature storage methods (default: false, only for versioned)
///
/// Example (versioned):
/// ```text
/// #[derive(Stored)]
/// #[stored(item_type = MyType, table = "my_table", namespace = "my_ns")]
/// pub struct MyRepository {
///     db: Surreal<Client>,
/// }
/// ```
///
/// Example (unversioned):
/// ```text
/// #[derive(Stored)]
/// #[stored(item_type = MyType, table = "my_table", namespace = "my_ns", versioned = false)]
/// pub struct MyRepository {
///     db: Surreal<Client>,
/// }
/// ```
///
/// Example (versioned with signatures):
/// ```text
/// #[derive(Stored)]
/// #[stored(item_type = KeyEvent, table = "key_events", namespace = "kels", signatures = true)]
/// pub struct KeyEventRepository {
///     db: Surreal<Client>,
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
    let mut namespace: Option<String> = None;
    let mut id_field = "said".to_string();
    let mut prefix_field = "prefix".to_string();
    let mut versioned = true;
    let mut signatures = false;

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
            } else if meta.path.is_ident("namespace") {
                meta.input.parse::<syn::Token![=]>()?;
                let lit: Lit = meta.input.parse()?;
                if let Lit::Str(s) = lit {
                    namespace = Some(s.value());
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
            } else if meta.path.is_ident("signatures") {
                meta.input.parse::<syn::Token![=]>()?;
                let lit: Lit = meta.input.parse()?;
                if let Lit::Bool(b) = lit {
                    signatures = b.value();
                }
            }
            Ok(())
        })
        .expect("Failed to parse #[stored(...)] attribute");

    let item_type = item_type.expect("Missing item_type in #[stored(...)]");
    let table_name = table_name.expect("Missing table in #[stored(...)]");
    let namespace = namespace.expect("Missing namespace in #[stored(...)]");

    // Convert field names to identifiers for use in generated code
    let id_field_ident = syn::Ident::new(&id_field, proc_macro2::Span::call_site());

    // Build query strings with the table name and prefix field baked in
    let get_latest_query = format!(
        "SELECT * FROM {} WHERE {} = $prefix ORDER BY version DESC LIMIT 1",
        table_name, prefix_field
    );
    let get_history_query = format!(
        "SELECT * FROM {} WHERE {} = $prefix ORDER BY version ASC",
        table_name, prefix_field
    );
    let exists_query = format!(
        "SELECT * FROM {} WHERE {} = $prefix LIMIT 1",
        table_name, prefix_field
    );

    // Generate the new() constructor
    let new_impl = quote! {
        impl #repo_name {
            pub async fn new(
                url: &str,
                database: &str,
                username: &str,
                password: &str,
            ) -> Result<Self, verifiable_storage::StorageError> {
                use surrealdb::engine::remote::ws::Ws;
                use surrealdb::opt::auth::Root;
                use surrealdb::Surreal;

                let db = Surreal::new::<Ws>(url).await
                    .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;
                db.signin(Root { username, password }).await
                    .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;
                db.use_ns(#namespace).use_db(database).await
                    .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;
                Ok(Self { db })
            }
        }
    };

    // Generate signature methods if enabled
    let signature_methods = if signatures {
        quote! {
            impl #repo_name {
                /// Store an item with its signature (item should already have SAID computed)
                pub async fn create_with_signatures(
                    &self,
                    item: #item_type,
                    signatures: Vec<adns::EventSignature>
                ) -> Result<#item_type, verifiable_storage::StorageError> {
                    use verifiable_storage::SelfAddressed;

                    // Store the signatures separately
                    for signature in &signatures {
                        let sig = adns::EventSignature::create(
                            item.#id_field_ident.clone(),
                            signature.public_key.clone(),
                            signature.signature.clone(),
                        );
                        let _: Option<adns::EventSignature> = self.db
                            .create(("signatures", sig.said.clone()))
                            .content(sig)
                            .await
                            .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;
                    }

                    // Store the item
                    let _: Option<#item_type> = self.db
                        .create((#table_name, item.#id_field_ident.clone()))
                        .content(item.clone())
                        .await
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;

                    Ok(item)
                }

                /// Get the signature for an item by its SAID
                pub async fn get_signature_by_said(&self, said: &str) -> Result<Option<adns::EventSignature>, verifiable_storage::StorageError> {
                    let mut result: Vec<adns::EventSignature> = self.db
                        .query("SELECT * FROM signatures WHERE eventSaid = $said LIMIT 1")
                        .bind(("said", said.to_string()))
                        .await
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?
                        .take(0)
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;

                    Ok(result.pop())
                }

                /// Get signatures for multiple SAIDs in one query (returns multiple sigs per event for recovery)
                pub async fn get_signatures_by_saids(
                    &self,
                    saids: &[String],
                ) -> Result<std::collections::HashMap<String, Vec<adns::EventSignature>>, verifiable_storage::StorageError> {
                    let result: Vec<adns::EventSignature> = self.db
                        .query("SELECT * FROM signatures WHERE $saids CONTAINS eventSaid")
                        .bind(("saids", saids.to_vec()))
                        .await
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?
                        .take(0)
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;

                    let mut map: std::collections::HashMap<String, Vec<adns::EventSignature>> = std::collections::HashMap::new();
                    for sig in result {
                        map.entry(sig.event_said.clone()).or_default().push(sig);
                    }

                    Ok(map)
                }

                /// Get the full signed history for a prefix (items with signatures)
                pub async fn get_signed_history(
                    &self,
                    prefix: &str,
                ) -> Result<Vec<adns::SignedKeyEvent>, verifiable_storage::StorageError> {
                    use verifiable_storage::VersionedRepository;

                    let events = <Self as verifiable_storage::VersionedRepository<#item_type>>::get_history(self, prefix).await?;
                    let saids: Vec<String> = events.iter().map(|e| e.#id_field_ident.clone()).collect();
                    let signatures = self.get_signatures_by_saids(&saids).await?;

                    let mut signed_events = Vec::with_capacity(events.len());
                    for event in events {
                        let sigs = signatures.get(&event.#id_field_ident)
                            .ok_or_else(|| verifiable_storage::StorageError::StorageError(
                                format!("No signatures found for event {}", event.#id_field_ident)
                            ))?;
                        let sig_pairs: Vec<(String, String)> = sigs.iter()
                            .map(|s| (s.public_key.clone(), s.signature.clone()))
                            .collect();
                        signed_events.push(adns::SignedKeyEvent::from_signatures(event, sig_pairs));
                    }

                    Ok(signed_events)
                }

                /// Get the full KEL for a prefix as a Kel struct
                pub async fn get_kel(&self, prefix: &str) -> Result<adns::Kel, verifiable_storage::StorageError> {
                    let signed_events = self.get_signed_history(prefix).await?;
                    adns::Kel::from_events(signed_events, false)
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))
                }
            }
        }
    } else {
        quote! {}
    };

    let expanded = if versioned {
        // Generate VersionedRepository impl
        quote! {
            #new_impl

            #[async_trait::async_trait]
            impl verifiable_storage::VersionedRepository<#item_type> for #repo_name {
                async fn create(&self, mut item: #item_type) -> Result<#item_type, verifiable_storage::StorageError> {
                    use verifiable_storage::Versioned;
                    item.derive_prefix()?;
                    let _ = self.insert(item.clone()).await?;
                    Ok(item)
                }

                async fn update(&self, mut item: #item_type) -> Result<#item_type, verifiable_storage::StorageError> {
                    use verifiable_storage::Versioned;
                    item.increment()?;
                    let _ = self.insert(item.clone()).await?;
                    Ok(item)
                }

                async fn insert(&self, item: #item_type) -> Result<#item_type, verifiable_storage::StorageError> {
                    let _: Option<#item_type> = self.db
                        .create((#table_name, item.#id_field_ident.clone()))
                        .content(item.clone())
                        .await
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;
                    Ok(item)
                }

                async fn get_by_said(&self, said: &str) -> Result<Option<#item_type>, verifiable_storage::StorageError> {
                    let result: Option<#item_type> = self.db.select((#table_name, said)).await
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;
                    Ok(result)
                }

                async fn get_latest(&self, prefix: &str) -> Result<Option<#item_type>, verifiable_storage::StorageError> {
                    let mut result: Vec<#item_type> = self.db
                        .query(#get_latest_query)
                        .bind(("prefix", prefix.to_string()))
                        .await
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?
                        .take(0)
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;
                    Ok(result.pop())
                }

                async fn get_history(&self, prefix: &str) -> Result<Vec<#item_type>, verifiable_storage::StorageError> {
                    let mut response = self.db
                        .query(#get_history_query)
                        .bind(("prefix", prefix.to_string()))
                        .await
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;
                    let result: Vec<#item_type> = response.take(0)
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;
                    Ok(result)
                }

                async fn exists(&self, prefix: &str) -> Result<bool, verifiable_storage::StorageError> {
                    let result: Vec<#item_type> = self.db
                        .query(#exists_query)
                        .bind(("prefix", prefix.to_string()))
                        .await
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?
                        .take(0)
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;
                    Ok(!result.is_empty())
                }
            }

            #signature_methods
        }
    } else {
        // Generate UnversionedRepository impl
        quote! {
            #new_impl

            #[async_trait::async_trait]
            impl verifiable_storage::UnversionedRepository<#item_type> for #repo_name {
                async fn create(&self, mut item: #item_type) -> Result<#item_type, verifiable_storage::StorageError> {
                    use verifiable_storage::SelfAddressed;
                    item.derive_said()?;
                    let _ = self.insert(item.clone()).await?;
                    Ok(item)
                }

                async fn insert(&self, item: #item_type) -> Result<#item_type, verifiable_storage::StorageError> {
                    let _: Option<#item_type> = self.db
                        .create((#table_name, item.#id_field_ident.clone()))
                        .content(item.clone())
                        .await
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;
                    Ok(item)
                }

                async fn get_by_said(&self, said: &str) -> Result<Option<#item_type>, verifiable_storage::StorageError> {
                    let result: Option<#item_type> = self.db.select((#table_name, said)).await
                        .map_err(|e| verifiable_storage::StorageError::StorageError(e.to_string()))?;
                    Ok(result)
                }
            }

            #signature_methods
        }
    };

    TokenStream::from(expanded)
}

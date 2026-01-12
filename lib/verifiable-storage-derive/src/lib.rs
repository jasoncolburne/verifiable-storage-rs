use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Lit, parse_macro_input};

/// Convert snake_case to camelCase
fn to_camel_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = false;

    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    result
}

/// Check if a field has a specific attribute
fn has_attr(field: &syn::Field, attr_name: &str) -> bool {
    field
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident(attr_name))
}

/// Check if a field has #[column(skip)]
fn has_column_skip(field: &syn::Field) -> bool {
    for attr in &field.attrs {
        if attr.path().is_ident("column") {
            let mut skip = false;
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("skip") {
                    skip = true;
                }
                Ok(())
            });
            if skip {
                return true;
            }
        }
    }
    false
}

/// Get custom column name from #[column(name = "...")] or None
fn get_column_name(field: &syn::Field) -> Option<String> {
    for attr in &field.attrs {
        if attr.path().is_ident("column") {
            let mut name = None;
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    meta.input.parse::<syn::Token![=]>()?;
                    let lit: Lit = meta.input.parse()?;
                    if let Lit::Str(s) = lit {
                        name = Some(s.value());
                    }
                }
                Ok(())
            });
            if name.is_some() {
                return name;
            }
        }
    }
    None
}

/// Map Rust type to generic SQL type name
fn rust_type_to_sql_type(ty: &syn::Type) -> &'static str {
    let type_str = quote::quote!(#ty).to_string();
    // Remove spaces for easier matching
    let type_str = type_str.replace(' ', "");

    // Check for Option<T> - extract inner type
    let inner_type = if type_str.starts_with("Option<") && type_str.ends_with('>') {
        &type_str[7..type_str.len() - 1]
    } else {
        type_str.as_str()
    };

    match inner_type {
        // Datetime types
        s if s.contains("StorageDatetime") => "datetime",
        s if s.contains("DateTime") => "datetime",
        // Integer types
        "u64" | "i64" => "bigint",
        "u32" | "i32" | "usize" | "isize" => "integer",
        // Boolean
        "bool" => "boolean",
        // Default to text for String and everything else
        _ => "text",
    }
}

/// Parse #[storable(table = "...")] attribute and return table name
fn parse_storable_attr(input: &DeriveInput) -> Option<String> {
    for attr in &input.attrs {
        if attr.path().is_ident("storable") {
            let mut table_name = None;
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("table") {
                    meta.input.parse::<syn::Token![=]>()?;
                    let lit: Lit = meta.input.parse()?;
                    if let Lit::Str(s) = lit {
                        table_name = Some(s.value());
                    }
                }
                Ok(())
            });
            return table_name;
        }
    }
    None
}

/// Derive macro for SelfAddressed trait (and optionally Versioned)
///
/// Generates implementations for self-addressed types with content-based identifiers.
/// Requires a field marked with `#[said]` attribute.
///
/// If `#[prefix]`, `#[previous]`, and `#[version]` fields are present, also generates
/// `Versioned` trait implementation for version-chained types.
///
/// ## Generated methods
///
/// ### Always generated (inherent):
/// - `new(params...)` - Constructor excluding storage-managed fields
/// - `create(params...)` - Constructor that also derives SAID/prefix, returns `Result`
///
/// ### Always generated (SelfAddressed trait):
/// - `derive_said()` - Compute content-based SAID
/// - `verify_said()` - Verify SAID matches content
/// - `get_said()` - Get current SAID
///
/// ### Generated when versioned (Versioned trait):
/// - `derive_prefix()` - Compute prefix from inception SAID
/// - `verify_prefix()` - Verify prefix matches content
/// - `get_prefix()` - Get current prefix
/// - `increment()` - Increment version for updates
/// - `verify_unchanged(proposed)` - Check if proposed update has actual changes
/// - `get_version()`, `get_previous()`, `get_created_at()`, `set_created_at()`
///
/// ## Storage-managed fields
///
/// These fields are excluded from `new()` parameters and auto-initialized:
/// - `#[said]` - empty string (computed by `derive_said()` or `derive_prefix()`)
/// - `#[prefix]` - empty string (computed by `derive_prefix()`)
/// - `#[previous]` - None
/// - `#[version]` - 0
/// - `#[created_at]` - current timestamp
///
/// ## Example (unversioned)
///
/// ```text
/// #[derive(SelfAddressed)]
/// struct AuditRecord {
///     #[said]
///     pub said: String,
///     #[created_at]
///     pub recorded_at: StorageDatetime,
///     pub data: String,
/// }
/// // Use: let record = AuditRecord::create(data)?;
/// ```
///
/// ## Example (versioned)
///
/// ```text
/// #[derive(SelfAddressed)]
/// struct Domain {
///     #[said]
///     pub said: String,
///     #[prefix]
///     pub prefix: String,
///     #[previous]
///     pub previous: Option<String>,
///     #[version]
///     pub version: u64,
///     #[created_at]
///     pub created_at: StorageDatetime,
///     pub name: String,
/// }
/// // Use: let domain = Domain::create(name)?;
/// ```
#[proc_macro_derive(
    SelfAddressed,
    attributes(said, prefix, previous, version, created_at, storable, column)
)]
pub fn derive_self_addressed(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("SelfAddressed only supports structs with named fields"),
        },
        _ => panic!("SelfAddressed only supports structs"),
    };

    let said_field = fields
        .iter()
        .find(|f| has_attr(f, "said"))
        .expect("No field marked with #[said] attribute found");
    let said_field_name = said_field.ident.as_ref().unwrap();

    // Check for versioned fields
    let prefix_field = fields.iter().find(|f| has_attr(f, "prefix"));
    let previous_field = fields.iter().find(|f| has_attr(f, "previous"));
    let version_field = fields.iter().find(|f| has_attr(f, "version"));
    let created_at_field = fields.iter().find(|f| has_attr(f, "created_at"));

    let is_versioned =
        prefix_field.is_some() && previous_field.is_some() && version_field.is_some();

    // Collect fields for new() method - exclude storage-managed fields
    let mut new_params = Vec::new();
    let mut new_param_names = Vec::new();
    let mut new_field_inits = Vec::new();

    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();
        let field_ty = &field.ty;

        if has_attr(field, "said") || has_attr(field, "prefix") {
            new_field_inits.push(quote! { #field_name: String::new() });
        } else if has_attr(field, "previous") {
            new_field_inits.push(quote! { #field_name: None });
        } else if has_attr(field, "version") {
            new_field_inits.push(quote! { #field_name: 0 });
        } else if has_attr(field, "created_at") {
            new_field_inits
                .push(quote! { #field_name: verifiable_storage::StorageDatetime::now() });
        } else {
            // Regular field - add as parameter
            new_params.push(quote! { #field_name: #field_ty });
            new_param_names.push(quote! { #field_name });
            new_field_inits.push(quote! { #field_name });
        }
    }

    // Generate create() - calls derive_prefix() for versioned, derive_said() for unversioned
    let create_derive_call = if is_versioned {
        quote! {
            use verifiable_storage::Versioned;
            item.derive_prefix()?;
        }
    } else {
        quote! {
            use verifiable_storage::SelfAddressed;
            item.derive_said()?;
        }
    };

    // Generate Versioned impl if applicable
    let versioned_impl = if is_versioned {
        let prefix_field_name = prefix_field.unwrap().ident.as_ref().unwrap();
        let previous_field_name = previous_field.unwrap().ident.as_ref().unwrap();
        let version_field_name = version_field.unwrap().ident.as_ref().unwrap();

        let created_at_get = if let Some(field) = created_at_field {
            let field_name = field.ident.as_ref().unwrap();
            quote! { Some(self.#field_name.clone()) }
        } else {
            quote! { None }
        };

        let created_at_set = if let Some(field) = created_at_field {
            let field_name = field.ident.as_ref().unwrap();
            quote! { self.#field_name = created_at.clone(); }
        } else {
            quote! {}
        };

        quote! {
            impl verifiable_storage::Versioned for #name {
                fn derive_prefix(&mut self) -> Result<(), verifiable_storage::StorageError> {
                    use verifiable_storage::SelfAddressed;
                    self.#prefix_field_name = "#".repeat(44);
                    self.derive_said()?;
                    self.#prefix_field_name = self.#said_field_name.clone();
                    Ok(())
                }

                fn verify_prefix(&self) -> Result<(), verifiable_storage::StorageError> {
                    use verifiable_storage::SelfAddressed;
                    let mut copy = self.clone();
                    copy.derive_prefix()?;
                    if copy.#said_field_name != self.#said_field_name || copy.#prefix_field_name != self.#prefix_field_name {
                        return Err(verifiable_storage::StorageError::InvalidSaid(format!(
                            "SAID prefix verification failed: expected said={}, prefix={}, got said={}, prefix={}",
                            self.#said_field_name, self.#prefix_field_name,
                            copy.#said_field_name, copy.#prefix_field_name
                        )));
                    }
                    Ok(())
                }

                fn get_prefix(&self) -> String {
                    self.#prefix_field_name.clone()
                }

                fn increment(&mut self) -> Result<(), verifiable_storage::StorageError> {
                    use verifiable_storage::SelfAddressed;
                    let old_id = self.#said_field_name.clone();
                    self.#previous_field_name = Some(old_id);
                    self.#version_field_name += 1;
                    self.set_created_at(verifiable_storage::StorageDatetime::now());
                    self.derive_said()?;
                    Ok(())
                }

                fn verify_unchanged(&self, proposed: &Self) -> Result<bool, verifiable_storage::StorageError> {
                    use verifiable_storage::SelfAddressed;
                    let mut next_if_unchanged = self.clone();
                    next_if_unchanged.#previous_field_name = Some(self.#said_field_name.clone());
                    next_if_unchanged.#version_field_name += 1;
                    next_if_unchanged.set_created_at(proposed.get_created_at().unwrap_or_else(verifiable_storage::StorageDatetime::now));
                    next_if_unchanged.derive_said()?;
                    Ok(next_if_unchanged.#said_field_name == proposed.#said_field_name)
                }

                fn get_version(&self) -> u64 {
                    self.#version_field_name
                }

                fn get_created_at(&self) -> Option<verifiable_storage::StorageDatetime> {
                    #created_at_get
                }

                fn set_created_at(&mut self, created_at: verifiable_storage::StorageDatetime) {
                    #created_at_set
                }

                fn get_previous(&self) -> Option<String> {
                    self.#previous_field_name.clone()
                }
            }

            impl PartialEq for #name {
                fn eq(&self, other: &Self) -> bool {
                    self.#prefix_field_name == other.#prefix_field_name
                        && self.#version_field_name == other.#version_field_name
                }
            }

            impl Eq for #name {}

            impl PartialOrd for #name {
                fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                    Some(self.cmp(other))
                }
            }

            impl Ord for #name {
                fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                    (&self.#prefix_field_name, self.#version_field_name)
                        .cmp(&(&other.#prefix_field_name, other.#version_field_name))
                }
            }
        }
    } else {
        quote! {}
    };

    // Generate Storable impl if #[storable(table = "...")] is present
    let storable_impl = if let Some(table_name) = parse_storable_attr(&input) {
        // Collect column names, types, and JSON keys for all non-skipped fields
        let mut column_names: Vec<String> = Vec::new();
        let mut column_types: Vec<&'static str> = Vec::new();
        let mut json_keys: Vec<String> = Vec::new();

        for field in fields.iter() {
            if has_column_skip(field) {
                continue;
            }

            let field_name = field.ident.as_ref().unwrap();
            let col_name = get_column_name(field).unwrap_or_else(|| field_name.to_string());
            let col_type = rust_type_to_sql_type(&field.ty);
            let json_key = to_camel_case(&field_name.to_string());

            column_names.push(col_name);
            column_types.push(col_type);
            json_keys.push(json_key);
        }

        // Generate INSERT SQL: INSERT INTO table (col1, col2, ...) VALUES ($1, $2, ...)
        let columns_str = column_names.join(", ");
        let placeholders: Vec<String> = (1..=column_names.len())
            .map(|i| format!("${}", i))
            .collect();
        let placeholders_str = placeholders.join(", ");
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name, columns_str, placeholders_str
        );

        // Generate SELECT SQLs
        let select_all_sql = format!("SELECT * FROM {}", table_name);
        let select_by_id_sql = format!("SELECT * FROM {} WHERE said = $1", table_name);

        // Column names as static array
        let column_count = column_names.len();
        let column_literals: Vec<_> = column_names.iter().map(|s| s.as_str()).collect();
        let column_type_literals: Vec<_> = column_types.to_vec();
        let json_key_literals: Vec<_> = json_keys.iter().map(|s| s.as_str()).collect();

        quote! {
            impl verifiable_storage::Storable for #name {
                fn table_name() -> &'static str {
                    #table_name
                }

                fn columns() -> &'static [&'static str] {
                    &[#(#column_literals),*]
                }

                fn column_types() -> &'static [&'static str] {
                    &[#(#column_type_literals),*]
                }

                fn json_keys() -> &'static [&'static str] {
                    &[#(#json_key_literals),*]
                }

                fn insert_sql() -> &'static str {
                    #insert_sql
                }

                fn select_all_sql() -> &'static str {
                    #select_all_sql
                }

                fn select_by_id_sql() -> &'static str {
                    #select_by_id_sql
                }

                fn column_count() -> usize {
                    #column_count
                }

                fn id(&self) -> &str {
                    &self.#said_field_name
                }

                fn is_versioned() -> bool {
                    #is_versioned
                }
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        impl #name {
            /// Create a new instance with storage-managed fields initialized to defaults.
            ///
            /// Storage-managed fields are automatically set:
            /// - `said`: empty string (compute with `derive_said()` or `derive_prefix()`)
            /// - `prefix`: empty string (compute with `derive_prefix()` for versioned types)
            /// - `previous`: None
            /// - `version`: 0
            /// - `created_at`: current timestamp
            pub fn new(#(#new_params),*) -> Self {
                Self {
                    #(#new_field_inits),*
                }
            }

            /// Create a new fully-initialized instance with SAID/prefix computed.
            ///
            /// This is the preferred way to create new instances. It:
            /// 1. Creates the instance with `new()` (sets created_at to now())
            /// 2. Computes the SAID (and prefix for versioned types)
            /// 3. Returns the fully-initialized instance
            pub fn create(#(#new_params),*) -> Result<Self, verifiable_storage::StorageError> {
                let mut item = Self::new(#(#new_param_names),*);
                #create_derive_call
                Ok(item)
            }
        }

        impl verifiable_storage::SelfAddressed for #name {
            fn derive_said(&mut self) -> Result<(), verifiable_storage::StorageError> {
                self.#said_field_name = "#".repeat(44);
                self.#said_field_name = verifiable_storage::compute_said(self)?;
                Ok(())
            }

            fn verify_said(&self) -> Result<(), verifiable_storage::StorageError> {
                let mut copy = self.clone();
                copy.derive_said()?;
                if copy.#said_field_name != self.#said_field_name {
                    return Err(verifiable_storage::StorageError::InvalidSaid(format!(
                        "SAID verification failed: expected {}, got {}",
                        self.#said_field_name, copy.#said_field_name
                    )));
                }
                Ok(())
            }

            fn get_said(&self) -> String {
                self.#said_field_name.clone()
            }
        }

        #versioned_impl

        #storable_impl
    };

    TokenStream::from(expanded)
}

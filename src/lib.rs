use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use bson::{doc, Bson, Document};
use futures::stream::StreamExt;
use mongodb::Database;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

mod tests;

pub type JsonMap = Map<String, Value>;
#[derive(Debug, Serialize, Deserialize)]

pub enum RequestDataKey {
    Body(String),
    Query(String),
    Path(),
}

#[derive(Clone)]
pub struct DuplicatePolicy {
    field: String,
    break_reasponse: axum::response::Json<String>,
}

impl DuplicatePolicy {
    pub fn new(field: String, break_reasponse: axum::response::Json<String>) -> Self {
        DuplicatePolicy {
            field,
            break_reasponse,
        }
    }
}

pub trait RouterExtension {
    fn add_find_route(
        self,
        path: &str,
        collection: &str,
        query: Document,
        query_param_name: Option<String>,
        is_param_number: Option<bool>,
        limit: Option<i64>,
    ) -> Self;

    fn add_find_route_with_field(
        self,
        path: &str,
        collection: &str,
        compare_field: RequestDataKey,
        document_key: &str,
        limit: Option<i64>,
    ) -> Self;
    fn add_find_by_body_route(self, path: &str, collection: &str, limit: Option<i64>) -> Self;
    fn add_create_new_from_body_route(
        self,
        path: &str,
        collection: &str,
        validator: fn(&JsonMap) -> bool,
        duplicate_field_policies: Option<Vec<DuplicatePolicy>>,
    ) -> Self;

    fn add_create_new_of_from_body_route<T: DeserializeOwned>(
        self,
        path: &str,
        collection: &str,
    ) -> Self;

    fn add_aggregate_route(
        self,
        path: &str,
        collection: &str,
        pipeline: Vec<Document>,
        query_param_name: Option<String>,
    ) -> Self;

    fn add_update_route(
        self,
        path: &str,
        coll_name: String,
        field_name: String,
        is_param_number: bool,
    ) -> Self;

    fn add_find_and_process_by_body_route(
        self,
        path: &str,
        collection: &str,
        next: fn(Option<Document>, JsonMap) -> (StatusCode, Json<String>),
        rm_query_field: Option<String>,
    ) -> Self;
}

impl RouterExtension for Router {
    fn add_find_route(
        self,
        path: &str,
        collection: &str,
        query: Document,
        query_param_name: Option<String>,
        is_param_number: Option<bool>,
        limit: Option<i64>,
    ) -> Self {
        let str = collection.to_string();
        if let Some(param_name) = query_param_name {
            if is_param_number.is_none() || !is_param_number.unwrap() {
                return self.route(
                    format!("{}/:data", path).as_str(),
                    get(move |data, Path(str_param): Path<String>| async move {
                        println!("Calling mongodb_find with param: {}", str_param);
                        mongodb_find(data, str, query, Some(doc! {param_name : str_param}), limit)
                            .await
                    }),
                );
            }
            return self.route(
                format!("{}/:data", path).as_str(),
                get(move |data, Path(int_param): Path<f32>| async move {
                    mongodb_find(data, str, query, Some(doc! {param_name : int_param}), limit).await
                }),
            );
        }
        self.route(
            path,
            get(move |data| async move { mongodb_find(data, str, query, None, limit).await }),
        )
    }

    fn add_find_route_with_field(
        self,
        path: &str,
        collection: &str,
        compare_field: RequestDataKey,
        document_key: &str,
        limit: Option<i64>,
    ) -> Self {
        let col_name = collection.to_string();
        let document_key = document_key.to_string();
        return match compare_field {
            RequestDataKey::Body(key) => {
                self.route(path, get(move |database, body: Json<JsonMap>| async move {
                    let mut query = doc! {};
                    match body.0.get(&key) {
                        Some(value) => {
                            query.insert(&document_key, serde_json::from_value::<Bson>(value.to_owned()).unwrap());
                        }
                        None => { return  (StatusCode::BAD_REQUEST, Json(vec![doc! {"error" : format!("Please specify body parameter: {}", &key)}])) }
                    }
                    mongodb_find(database, col_name, query, None, limit).await
                }))
            }
            RequestDataKey::Query(key) => {
                self.route(path, get(move |database, Query(query): Query<JsonMap>| async move {
                    let mut query_doc = doc! {};
                    match query.get(&key) {
                        Some(value) => {
                            query_doc.insert(&document_key, serde_json::from_value::<Bson>(value.to_owned()).unwrap());
                        }
                        None => { return  (StatusCode::BAD_REQUEST, Json(vec![doc! {"error" : format!("Please specify query parameter: {}", &key)}])) }
                    }
                    mongodb_find(database, col_name, query_doc, None, limit).await
                }))
            }
            RequestDataKey::Path() => {
                self.route(format!("{}/:path_data", path).as_str(), get(move |database, axum::extract::Path(path_data): axum::extract::Path<Value>| async move {
                    let mut query_doc = doc! {};
                    query_doc.insert(document_key, serde_json::from_value::<Bson>(path_data.to_owned()).unwrap());

                    mongodb_find(database, col_name, query_doc, None, limit).await
                }))
            }
        };
    }

    fn add_find_by_body_route(self, path: &str, collection: &str, limit: Option<i64>) -> Self {
        let col_name = collection.to_string();
        self.route(
            path,
            get(move |database, body: Json<JsonMap>| async move {
                let query = bson::to_document(&body.0).unwrap();
                mongodb_find(database, col_name, query, None, limit).await
            }),
        )
    }

    fn add_create_new_from_body_route(
        self,
        path: &str,
        collection: &str,
        validator: fn(&JsonMap) -> bool,
        duplicate_field_policies: Option<Vec<DuplicatePolicy>>,
    ) -> Self {
        let str = collection.to_string();
        println!("Adding route: {}", path);
        self.route(
            path,
            post(
                move |database: Extension<Database>, body: Json<JsonMap>| async move {
                    if validator(&body.0) {
                        if let Some(policies) = duplicate_field_policies {
                            let col = database.collection::<Document>(&str);
                            for policy in policies {
                                let field = policy.field;
                                let value = body.0.get(&field).unwrap();
                                let query = doc! {&field : bson::to_bson(&value).unwrap()};
                                let result = col.find_one(query, None).await.unwrap();
                                if result.is_some() {
                                    return (StatusCode::BAD_REQUEST, policy.break_reasponse);
                                }
                            }
                        }
                        mongodb_create_new_from_body(database, body.0, str).await
                    } else {
                        (
                            StatusCode::BAD_REQUEST,
                            axum::response::Json("Invalid body".to_owned()),
                        )
                    }
                },
            ),
        )
    }

    fn add_create_new_of_from_body_route<T: DeserializeOwned>(
        self,
        path: &str,
        collection: &str,
    ) -> Self {
        self.add_create_new_from_body_route(
            path,
            collection,
            |body| is_valid_json_for::<T>(body),
            None,
        )
    }

    fn add_find_and_process_by_body_route(
        self,
        path: &str,
        collection: &str,
        next: fn(Option<Document>, JsonMap) -> (StatusCode, Json<String>),
        rm_query_field: Option<String>,
    ) -> Self {
        let collection_name = collection.to_string();
        self.route(
            path,
            get(
                move |db: Extension<Database>, body: Json<JsonMap>| async move {
                    //if !is_valid_json_for::<T>(&body.0) {
                    //  return (StatusCode::BAD_REQUEST, Json("Invalid body".to_owned()));
                    //}
                    let col = db.collection::<Document>(&collection_name);
                    let mut query = bson::to_document(&body.0).unwrap();
                    if let Some(field) = rm_query_field {
                        query.remove(field);
                    }
                    let result = col.find_one(query, None).await.unwrap();

                    //let body = serde_json::from_value::<T>(serde_json::to_value(&body.0).unwrap())
                    //  .unwrap();
                    next(result, body.0)
                },
            ),
        )
    }

    fn add_aggregate_route(
        self,
        path: &str,
        collection: &str,
        pipeline: Vec<Document>,
        query_param_name: Option<String>,
    ) -> Self {
        let str = collection.to_string();
        if let Some(param_name) = query_param_name {
            return self.route(
                format!("{}/:data", path).as_str(),
                get(move |data, Path(str_param): Path<String>| async move {
                    println!("Calling mongodb_aggregate with param: {}", str_param);
                    mongodb_aggregate(data, str, pipeline, Some(doc! {param_name : str_param}))
                        .await
                }),
            );
        }
        self.route(
            path,
            get(move |data| async move { mongodb_aggregate(data, str, pipeline, None).await }),
        )
    }

    fn add_update_route(
        self,
        path: &str,
        coll_name: String,
        field_name: String,
        is_param_number: bool,
    ) -> Self {
        self.route(
            format!("{}/:replace_val", path).as_str(),
            axum::routing::post(
                move |data, Path(replace_val): Path<String>, body: Json<JsonMap>| async move {
                    let replace_val = if is_param_number {
                        // If it's a numeric parameter, try to parse it as f64
                        match replace_val.parse::<f64>() {
                            Ok(num) => Bson::Double(num),
                            Err(_) => Bson::String(replace_val), // Treat non-numeric as string
                        }
                    } else {
                        Bson::String(replace_val)
                    };

                    mongodb_find_and_update(
                        data,
                        coll_name.clone(),
                        serde_json::from_value::<Document>(serde_json::to_value(&body.0).unwrap())
                            .unwrap(),
                        field_name.clone(),
                        replace_val,
                    )
                    .await
                },
            ),
        )
    }
}

pub fn is_valid_json_for<T: DeserializeOwned>(json: &JsonMap) -> bool {
    let json_value: Value = Value::Object(json.to_owned());
    let res = serde_json::from_value::<T>(json_value);

    res.is_ok()
}

async fn mongodb_find(
    Extension(db): Extension<mongodb::Database>,
    collection_name: String,
    query: Document,
    let_vars: Option<Document>,
    limit: Option<i64>,
) -> (StatusCode, Json<Vec<Document>>) {
    let col = db.collection::<Document>(&collection_name.to_owned());
    println!("collection: {}", collection_name);

    let mut find_options = mongodb::options::FindOptions::builder()
        .projection(doc! {"_id" : 0})
        .build();
    find_options.let_vars = let_vars;
    find_options.limit = limit;
    let result = col
        .find(query, find_options)
        .await
        .unwrap()
        .map(|doc| doc.unwrap())
        .collect::<Vec<Document>>()
        .await;
    (StatusCode::OK, Json(result))
}

async fn mongodb_aggregate(
    Extension(db): Extension<mongodb::Database>,
    collection_name: String,
    pipeline: Vec<Document>,
    let_vars: Option<Document>,
) -> (StatusCode, Json<Vec<Document>>) {
    let col = db.collection::<Document>(&collection_name.to_owned());
    println!("collection: {}", collection_name);
    let mut aggregate_options = mongodb::options::AggregateOptions::builder().build();
    aggregate_options.let_vars = let_vars;
    let result = col
        .aggregate(pipeline, aggregate_options)
        .await
        .unwrap()
        .map(|doc| doc.unwrap())
        .collect::<Vec<Document>>()
        .await;
    (StatusCode::OK, Json(result))
}

async fn mongodb_create_new_from_body(
    Extension(db): Extension<mongodb::Database>,
    json: JsonMap,
    collection_name: String,
) -> (StatusCode, axum::response::Json<String>) {
    let mut doc = bson::to_document(&json).unwrap();
    convert_int64_to_int32(&mut doc);

    let col = db.collection::<Document>(&collection_name);
    let result = col.insert_one(&doc, None).await.unwrap();
    (
        StatusCode::CREATED,
        axum::response::Json(format!(
            "Created new document with id: {}",
            result.inserted_id
        )),
    )
}

async fn mongodb_find_and_update(
    Extension(db): Extension<mongodb::Database>,
    coll_name: String,
    find_query: Document,
    field_name: String,
    replace_val: Bson,
) -> (StatusCode, String) {
    let collection = db.collection::<Document>(&coll_name);

    // Find the document based on the find query
    let filter = find_query;
    let update = doc! { "$set": { &field_name: replace_val } };
    let options = mongodb::options::FindOneAndUpdateOptions::builder()
        .return_document(mongodb::options::ReturnDocument::After)
        .build();

    if let Some(updated_doc) = collection
        .find_one_and_update(filter, update, options)
        .await
        .unwrap()
    {
        (
            StatusCode::OK,
            format!("Field updated successfully: {:?}", updated_doc),
        )
    } else {
        (StatusCode::BAD_REQUEST, "Document not found".to_owned())
    }
}

fn convert_int64_to_int32(doc: &mut Document) {
    for (_, value) in doc.iter_mut() {
        if let Bson::Int64(val) = value {
            // Attempt to convert to i32, if successful replace the value
            if let Ok(val_i32) = (*val).try_into() {
                *value = Bson::Int32(val_i32);
            }
        }
    }
}

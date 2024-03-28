use crate::{is_valid_json_for, JsonMap};
use axum::http::StatusCode;
use bson::Document;
use serde::Deserialize;
use std::time::Duration;
use std::{fmt::format, process::exit};
use tokio::spawn;
use tokio::time::sleep;

#[derive(Deserialize)]
struct Team {
    name: String,
    goals: i32,
    wins: i32,
    loses: i32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{quit, validate_team, Team};
    use crate::{JsonMap, RequestDataKey, RouterExtension};
    use axum::routing::get;
    use axum::{Extension, Router};
    use bson::doc;
    use mongodb::Client;
    use serde::Serialize;
    use std::net::SocketAddr;

    fn process_team(
        team: Option<mongodb::bson::Document>,
        body: JsonMap,
    ) -> (axum::http::StatusCode, axum::response::Json<String>) {
        println!("Body: {:?}", body);
        match team {
            Some(team) => {
                let name = team.get("name").unwrap().as_str().unwrap();
                let wins = team.get("wins").unwrap().as_i32().unwrap();
                let test = body.get("test").unwrap().as_str().unwrap();
                return (
                    axum::http::StatusCode::OK,
                    axum::response::Json(format!(
                        "{}::llä on {} voittoa, testi: {}",
                        name, wins, test
                    )),
                );
            }
            None => (
                axum::http::StatusCode::NOT_FOUND,
                axum::response::Json("Team not found".to_string()),
            ),
        }
    }

    #[tokio::test]
    async fn server_test() {
        let pipeline = vec![
            doc! {
                "$match": {
                    "$expr": {
                        "$or": [
                            { "$eq": ["$homeTeamName", "$$input"] },
                            { "$eq": ["$awayTeamName", "$$input"] }
                        ]
                    }
                }
            },
            doc! {
                "$group": {
                    "_id": null,
                    "totalGoals": {
                        "$sum": {
                            "$cond": {
                                "if": { "$eq": ["$homeTeamName", "$$input"] },
                                "then": "$homeTeamGoals",
                                "else": "$awayTeamGoals"
                            }
                        }
                    }
                }
            },
            doc! {
                "$project": {
                    "_id": 0,  // Exclude the _id field
                    "totalGoals": 1
                }
            },
        ];

        let mongo_client = Client::with_uri_str(
            "mongodb+srv://Iika:Iika2010@cluster0.lbqqgpm.mongodb.net/?retryWrites=true&w=majority",
        )
        .await
        .unwrap();
        let app = Router::new()
            .route("/", get(|| async { "Hello, World!" }))
            .route("/quit", get(quit))
            .add_find_route(
                "/find",
                "teams",
                doc! {"$expr": { "$eq" : ["$goals", "$$input"]}},
                Some("input".to_string()),
                Some(true),
                Some(5),
            )
            .add_create_new_from_body_route(
                "/create_new",
                "teams",
                validate_team,
                Some(vec![crate::DuplicatePolicy::new(
                    "name".to_owned(),
                    axum::response::Json("Name must be unique".to_owned()),
                )]),
            )
            .add_create_new_of_from_body_route::<Team>("/create_new_of", "teams")
            .add_find_route_with_field(
                "/find_by_name",
                "teams",
                RequestDataKey::Path(),
                "name",
                None,
            )
            .add_find_by_body_route("/find_by_body", "teams", None)
            .add_aggregate_route("/aggregate", "matches", pipeline, Some("input".to_string()))
            .add_find_and_process_by_body_route(
                "/fp",
                "teams",
                process_team,
                Some("test".to_string()),
            )
            .with_state(mongo_client.database("main"));
        let addr = SocketAddr::from(([127, 0, 0, 1], 8081));
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    }
}

fn validate_team(body: &JsonMap) -> bool {
    is_valid_json_for::<Team>(body) && !body.get("name").unwrap().as_str().unwrap().is_empty()
}

async fn quit() -> String {
    spawn(async {
        sleep(Duration::from_secs(3)).await;
        exit(0);
    });

    "Exiting...".to_owned()
}

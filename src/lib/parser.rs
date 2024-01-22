use serde_json::{self, Error, Value};

pub fn parser_json(s: String) -> Result<(String, String), Error> {
    let json_data: Value = serde_json::from_str(&s)?;
    Ok((
        json_data[0]["method"].to_string(),
        json_data[0]["topic"].to_string(),
    ))
}

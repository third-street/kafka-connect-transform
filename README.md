# kafka-connect-transform

## Single message transforms for Kafka Connect

This is a collection of Single Message Transforms for Kafka Connect. So far it contains:
* CharTrimTransform

### CharTrimTransform

This transformer will trim the character from the first and last character on the string field if present.

Name | Description | Type | Valid values | Importance
---- | ----------- | ---- | ------------ | ------------
field | The field containing the string to trim, or if empty, it will either trim the entire value if a string or every string field in the data record. | String |  | High
character | Character to trim from field value. | String | | High

#### Examples

Let's say you have a field FIELD1 that can have value: "123abc123" and you want to trim the double-quotes. In that case your connector config would look like (showing only transforms part):
```
"name": "my-connector-trims-quotes",
  "config": {
	"transforms": "trim_double_quotes",		
	"transforms.trim_double_quotes.type": "com.thirdstreet.transform.CharTrimTransform$Value",
	"transforms.trim_double_quotes.field": "FIELD1",
	"transforms.trim_double_quotes.character": "\""
  }
} 
```

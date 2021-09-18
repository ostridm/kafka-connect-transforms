Kafka Connect SMT to perform regex transform on field content

[![CircleCI](https://circleci.com/gh/ostridm/kafka-connect-transforms/tree/main.svg?style=svg)](https://circleci.com/gh/ostridm/kafka-connect-transforms/tree/main)

This SMT supports regex transform of a  Key or Value field

Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`name`| Field name | String | `` | High |
|`regex`| Filed content regex pattern | String | `` | High |
|`replacement`| Replacement string to use if field content has matched to pattern | String | `` | High |
|`default`| Replacement string to use if regex doesn't match | String | `` | High |

Regex pattern is internally compiled

Example on how to add to your connector:
```
transforms=regexrewrite
transforms.regexrewrite.type=com.github.ostridm.kafka.connect.transforms.RegexRewrite$Value
transforms.regexrewrite.name="hash"
transforms.regexrewrite.name="([0-9a-f]{40}|)"
transforms.regexrewrite.replacement="{\"hash\":\"$1\", \"algorithm\":\"SHA-1\"}}"
transforms.regexrewrite.default="{\"hash\":\"\", \"algorithm\":\"SHA-1\"}}"
```
Lots borrowed from the Apache Kafka® `TimestampConverter` SMT
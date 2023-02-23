ALTER TABLE foo ADD COLUMN baz "Mixed Case"
  Default ('foo'::"BarBazBaq") ON UPDATe ('baz':::"With Emojis too 👍"), ADD COLUMN bar "DefaultDB"."👎"."Mixed Case"
  Default ('foo'::"DefaultDB".public."Mixed Case") ON UPDATe ('baz':::"👎"."Mixed Case")

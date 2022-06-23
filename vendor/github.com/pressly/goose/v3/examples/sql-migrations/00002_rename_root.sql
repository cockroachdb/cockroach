-- +goose Up
-- +goose StatementBegin
UPDATE users SET username='admin' WHERE username='root';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
UPDATE users SET username='root' WHERE username='admin';
-- +goose StatementEnd

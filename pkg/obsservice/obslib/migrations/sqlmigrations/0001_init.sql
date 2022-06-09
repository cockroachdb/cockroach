-- +goose Up
CREATE TABLE events();

-- +goose Down
DROP TABLE events;

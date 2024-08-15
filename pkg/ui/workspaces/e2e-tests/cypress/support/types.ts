export enum SQLPrivilege {
  ADMIN = "ADMIN",
  VIEWACTIVITY = "VIEWACTIVITY",
  NONE = "NONE",
}

export type User = {
  username: string;
  password: string;
  sqlPrivileges: SQLPrivilege[];
};

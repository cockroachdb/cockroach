// based on the types in https://github.com/cockroachlabs/registration/blob/master/db.go
export interface Version {
  version: string;
  detail: string;
}

export interface VersionList {
  details: Version[];
}

export interface VersionStatus {
  error: boolean;
  message: string;
  laterVersions: VersionList;
}

interface RegistrationData {
  first_name: string;
  last_name: string;
  company: string;
  email: string;
  product_updates: boolean;
}

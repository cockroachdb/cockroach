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

interface RegisterationData {
  first_name: string;
  last_name: string;
  company: string;
  email: string;
  product_updates: boolean;
}

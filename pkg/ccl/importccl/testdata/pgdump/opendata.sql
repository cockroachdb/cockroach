-- opentrials-api-2018-04-01.dump from https://explorer.opentrials.net/data
-- extracted with: pg_restore -s -f opendata.sql opentrials-api-2018-04-01.dump

--
-- PostgreSQL database dump
--

-- Dumped from database version 9.4.15
-- Dumped by pg_dump version 9.4.12

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: conditions; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE conditions (
    id uuid NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    source_id text,
    slug text,
    description text,
    icdcm_code text
);


ALTER TABLE public.conditions OWNER TO database;

--
-- Name: document_categories; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE document_categories (
    id integer NOT NULL,
    name text NOT NULL,
    "group" text
);


ALTER TABLE public.document_categories OWNER TO database;

--
-- Name: documents; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE documents (
    id uuid NOT NULL,
    source_id text,
    name text NOT NULL,
    fda_approval_id text,
    file_id uuid,
    source_url text,
    document_category_id integer NOT NULL,
    CONSTRAINT file_id_xor_source_url_check CHECK ((((file_id IS NULL) AND (source_url IS NOT NULL)) OR ((file_id IS NOT NULL) AND (source_url IS NULL))))
);


ALTER TABLE public.documents OWNER TO database;

--
-- Name: fda_applications; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE fda_applications (
    id text NOT NULL,
    organisation_id uuid,
    drug_name text,
    active_ingredients text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.fda_applications OWNER TO database;

--
-- Name: fda_approvals; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE fda_approvals (
    id text NOT NULL,
    supplement_number integer NOT NULL,
    type text NOT NULL,
    action_date date NOT NULL,
    notes text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    fda_application_id text NOT NULL
);


ALTER TABLE public.fda_approvals OWNER TO database;

--
-- Name: files; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE files (
    id uuid NOT NULL,
    documentcloud_id text,
    sha1 text NOT NULL,
    source_url text NOT NULL,
    pages text[]
);


ALTER TABLE public.files OWNER TO database;

--
-- Name: interventions; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE interventions (
    id uuid NOT NULL,
    name text NOT NULL,
    type text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    source_id text,
    slug text,
    description text,
    icdpcs_code text,
    ndc_code text,
    fda_application_id text,
    CONSTRAINT interventions_type_check CHECK ((type = ANY (ARRAY['drug'::text, 'procedure'::text, 'other'::text])))
);


ALTER TABLE public.interventions OWNER TO database;

--
-- Name: knex_migrations; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE knex_migrations (
    id integer NOT NULL,
    name character varying(255),
    batch integer,
    migration_time timestamp with time zone
);


ALTER TABLE public.knex_migrations OWNER TO database;

--
-- Name: knex_migrations_id_seq; Type: SEQUENCE; Schema: public; Owner: database
--

CREATE SEQUENCE knex_migrations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.knex_migrations_id_seq OWNER TO database;

--
-- Name: knex_migrations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: database
--

ALTER SEQUENCE knex_migrations_id_seq OWNED BY knex_migrations.id;


--
-- Name: knex_migrations_lock; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE knex_migrations_lock (
    is_locked integer
);


ALTER TABLE public.knex_migrations_lock OWNER TO database;

--
-- Name: locations; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE locations (
    id uuid NOT NULL,
    name text NOT NULL,
    type text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    source_id text,
    slug text,
    CONSTRAINT locations_type_check CHECK ((type = ANY (ARRAY['country'::text, 'city'::text, 'other'::text])))
);


ALTER TABLE public.locations OWNER TO database;

--
-- Name: organizations; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE organizations (
    id uuid NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    source_id text,
    slug text
);


ALTER TABLE public.organizations OWNER TO database;

--
-- Name: persons; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE persons (
    id uuid NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    source_id text,
    slug text
);


ALTER TABLE public.persons OWNER TO database;

--
-- Name: publications; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE publications (
    id uuid NOT NULL,
    source_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    source_url text NOT NULL,
    title text NOT NULL,
    abstract text NOT NULL,
    authors text[],
    journal text,
    date date,
    slug text
);


ALTER TABLE public.publications OWNER TO database;

--
-- Name: records; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE records (
    id uuid NOT NULL,
    source_id text NOT NULL,
    source_url text NOT NULL,
    identifiers jsonb NOT NULL,
    registration_date date,
    public_title text NOT NULL,
    brief_summary text,
    scientific_title text,
    description text,
    recruitment_status text,
    eligibility_criteria jsonb,
    target_sample_size integer,
    first_enrollment_date date,
    study_type text,
    study_design text,
    study_phase text[],
    primary_outcomes jsonb,
    secondary_outcomes jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    has_published_results boolean,
    gender text,
    trial_id uuid,
    status text,
    completion_date date,
    results_exemption_date date,
    last_verification_date date,
    is_primary boolean DEFAULT false NOT NULL,
    age_range jsonb,
    CONSTRAINT records_recruitment_status_check CHECK ((recruitment_status = ANY (ARRAY['recruiting'::text, 'not_recruiting'::text, 'unknown'::text, 'other'::text]))),
    CONSTRAINT records_status_check CHECK ((status = ANY (ARRAY['ongoing'::text, 'withdrawn'::text, 'suspended'::text, 'terminated'::text, 'complete'::text, 'unknown'::text, 'other'::text]))),
    CONSTRAINT trialrecords_gender_check CHECK ((gender = ANY (ARRAY['both'::text, 'male'::text, 'female'::text])))
);


ALTER TABLE public.records OWNER TO database;

--
-- Name: risk_of_bias_criteria; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE risk_of_bias_criteria (
    id uuid NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.risk_of_bias_criteria OWNER TO database;

--
-- Name: risk_of_biases; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE risk_of_biases (
    id uuid NOT NULL,
    trial_id uuid NOT NULL,
    source_id text NOT NULL,
    source_url text NOT NULL,
    study_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.risk_of_biases OWNER TO database;

--
-- Name: risk_of_biases_risk_of_bias_criteria; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE risk_of_biases_risk_of_bias_criteria (
    risk_of_bias_id uuid NOT NULL,
    risk_of_bias_criteria_id uuid NOT NULL,
    value text NOT NULL,
    CONSTRAINT risk_of_biases_risk_of_bias_criteria_value_check CHECK ((value = ANY (ARRAY['yes'::text, 'no'::text, 'unknown'::text])))
);


ALTER TABLE public.risk_of_biases_risk_of_bias_criteria OWNER TO database;

--
-- Name: sources; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE sources (
    id text NOT NULL,
    name text NOT NULL,
    type text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    source_url text,
    terms_and_conditions_url text,
    CONSTRAINT sources_type_check CHECK ((type = ANY (ARRAY['register'::text, 'other'::text, 'journal'::text])))
);


ALTER TABLE public.sources OWNER TO database;

--
-- Name: trials; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE trials (
    id uuid NOT NULL,
    identifiers jsonb NOT NULL,
    registration_date date,
    public_title text NOT NULL,
    brief_summary text,
    scientific_title text,
    description text,
    recruitment_status text,
    eligibility_criteria jsonb,
    target_sample_size integer,
    first_enrollment_date date,
    study_type text,
    study_design text,
    study_phase text[],
    primary_outcomes jsonb,
    secondary_outcomes jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    has_published_results boolean,
    gender text,
    source_id text,
    status text,
    completion_date date,
    results_exemption_date date,
    age_range jsonb,
    CONSTRAINT trials_gender_check CHECK ((gender = ANY (ARRAY['both'::text, 'male'::text, 'female'::text]))),
    CONSTRAINT trials_recruitment_status_check CHECK ((recruitment_status = ANY (ARRAY['recruiting'::text, 'not_recruiting'::text, 'unknown'::text, 'other'::text]))),
    CONSTRAINT trials_status_check CHECK ((status = ANY (ARRAY['ongoing'::text, 'withdrawn'::text, 'suspended'::text, 'terminated'::text, 'complete'::text, 'unknown'::text, 'other'::text])))
);


ALTER TABLE public.trials OWNER TO database;

--
-- Name: trials_conditions; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE trials_conditions (
    trial_id uuid NOT NULL,
    condition_id uuid NOT NULL
);


ALTER TABLE public.trials_conditions OWNER TO database;

--
-- Name: trials_documents; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE trials_documents (
    trial_id uuid NOT NULL,
    document_id uuid NOT NULL
);


ALTER TABLE public.trials_documents OWNER TO database;

--
-- Name: trials_interventions; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE trials_interventions (
    trial_id uuid NOT NULL,
    intervention_id uuid NOT NULL
);


ALTER TABLE public.trials_interventions OWNER TO database;

--
-- Name: trials_locations; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE trials_locations (
    trial_id uuid NOT NULL,
    location_id uuid NOT NULL,
    role text,
    CONSTRAINT trials_locations_role_check CHECK ((role = ANY (ARRAY['recruitment_countries'::text, 'other'::text])))
);


ALTER TABLE public.trials_locations OWNER TO database;

--
-- Name: trials_organizations; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE trials_organizations (
    trial_id uuid NOT NULL,
    organisation_id uuid NOT NULL,
    role text,
    CONSTRAINT trials_organizations_role_check CHECK ((role = ANY (ARRAY['primary_sponsor'::text, 'sponsor'::text, 'funder'::text, 'other'::text])))
);


ALTER TABLE public.trials_organizations OWNER TO database;

--
-- Name: trials_persons; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE trials_persons (
    trial_id uuid NOT NULL,
    person_id uuid NOT NULL,
    role text,
    CONSTRAINT trials_persons_role_check CHECK ((role = ANY (ARRAY['principal_investigator'::text, 'public_queries'::text, 'scientific_queries'::text, 'other'::text])))
);


ALTER TABLE public.trials_persons OWNER TO database;

--
-- Name: trials_publications; Type: TABLE; Schema: public; Owner: database
--

CREATE TABLE trials_publications (
    trial_id uuid NOT NULL,
    publication_id uuid NOT NULL
);


ALTER TABLE public.trials_publications OWNER TO database;

--
-- Name: id; Type: DEFAULT; Schema: public; Owner: database
--

ALTER TABLE ONLY knex_migrations ALTER COLUMN id SET DEFAULT nextval('knex_migrations_id_seq'::regclass);


--
-- Name: document_categories_name_group_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY document_categories
    ADD CONSTRAINT document_categories_name_group_unique UNIQUE (name, "group");


--
-- Name: document_categories_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY document_categories
    ADD CONSTRAINT document_categories_pkey PRIMARY KEY (id);


--
-- Name: documents_fda_approval_id_file_id_name_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY documents
    ADD CONSTRAINT documents_fda_approval_id_file_id_name_unique UNIQUE (fda_approval_id, file_id, name);


--
-- Name: documents_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY documents
    ADD CONSTRAINT documents_pkey PRIMARY KEY (id);


--
-- Name: fda_applications_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY fda_applications
    ADD CONSTRAINT fda_applications_pkey PRIMARY KEY (id);


--
-- Name: fda_approvals_fda_application_id_supplement_number_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY fda_approvals
    ADD CONSTRAINT fda_approvals_fda_application_id_supplement_number_unique UNIQUE (fda_application_id, supplement_number);


--
-- Name: fda_approvals_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY fda_approvals
    ADD CONSTRAINT fda_approvals_pkey PRIMARY KEY (id);


--
-- Name: files_documentcloud_id_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY files
    ADD CONSTRAINT files_documentcloud_id_unique UNIQUE (documentcloud_id);


--
-- Name: files_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY files
    ADD CONSTRAINT files_pkey PRIMARY KEY (id);


--
-- Name: files_sha1_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY files
    ADD CONSTRAINT files_sha1_unique UNIQUE (sha1);


--
-- Name: files_source_url_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY files
    ADD CONSTRAINT files_source_url_unique UNIQUE (source_url);


--
-- Name: interventions_name_type_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY interventions
    ADD CONSTRAINT interventions_name_type_unique UNIQUE (name, type);


--
-- Name: interventions_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY interventions
    ADD CONSTRAINT interventions_pkey PRIMARY KEY (id);


--
-- Name: interventions_slug_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY interventions
    ADD CONSTRAINT interventions_slug_unique UNIQUE (slug);


--
-- Name: knex_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY knex_migrations
    ADD CONSTRAINT knex_migrations_pkey PRIMARY KEY (id);


--
-- Name: locations_name_type_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY locations
    ADD CONSTRAINT locations_name_type_unique UNIQUE (name, type);


--
-- Name: locations_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY locations
    ADD CONSTRAINT locations_pkey PRIMARY KEY (id);


--
-- Name: locations_slug_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY locations
    ADD CONSTRAINT locations_slug_unique UNIQUE (slug);


--
-- Name: organizations_name_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY organizations
    ADD CONSTRAINT organizations_name_unique UNIQUE (name);


--
-- Name: organizations_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY organizations
    ADD CONSTRAINT organizations_pkey PRIMARY KEY (id);


--
-- Name: organizations_slug_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY organizations
    ADD CONSTRAINT organizations_slug_unique UNIQUE (slug);


--
-- Name: persons_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY persons
    ADD CONSTRAINT persons_pkey PRIMARY KEY (id);


--
-- Name: persons_slug_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY persons
    ADD CONSTRAINT persons_slug_unique UNIQUE (slug);


--
-- Name: problems_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY conditions
    ADD CONSTRAINT problems_pkey PRIMARY KEY (id);


--
-- Name: problems_slug_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY conditions
    ADD CONSTRAINT problems_slug_unique UNIQUE (slug);


--
-- Name: publications_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY publications
    ADD CONSTRAINT publications_pkey PRIMARY KEY (id);


--
-- Name: publications_slug_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY publications
    ADD CONSTRAINT publications_slug_unique UNIQUE (slug);


--
-- Name: records_source_url_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY records
    ADD CONSTRAINT records_source_url_unique UNIQUE (source_url);


--
-- Name: risk_of_bias_criteria_name_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY risk_of_bias_criteria
    ADD CONSTRAINT risk_of_bias_criteria_name_unique UNIQUE (name);


--
-- Name: risk_of_bias_criteria_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY risk_of_bias_criteria
    ADD CONSTRAINT risk_of_bias_criteria_pkey PRIMARY KEY (id);


--
-- Name: risk_of_biases_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY risk_of_biases
    ADD CONSTRAINT risk_of_biases_pkey PRIMARY KEY (id);


--
-- Name: risk_of_biases_risk_of_bias_criteria_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY risk_of_biases_risk_of_bias_criteria
    ADD CONSTRAINT risk_of_biases_risk_of_bias_criteria_pkey PRIMARY KEY (risk_of_bias_id, risk_of_bias_criteria_id);


--
-- Name: risk_of_biases_study_id_source_url_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY risk_of_biases
    ADD CONSTRAINT risk_of_biases_study_id_source_url_unique UNIQUE (study_id, source_url);


--
-- Name: sources_name_type_unique; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY sources
    ADD CONSTRAINT sources_name_type_unique UNIQUE (name, type);


--
-- Name: sources_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY sources
    ADD CONSTRAINT sources_pkey PRIMARY KEY (id);


--
-- Name: trialrecords_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY records
    ADD CONSTRAINT trialrecords_pkey PRIMARY KEY (id);


--
-- Name: trials_documents_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_documents
    ADD CONSTRAINT trials_documents_pkey PRIMARY KEY (trial_id, document_id);


--
-- Name: trials_interventions_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_interventions
    ADD CONSTRAINT trials_interventions_pkey PRIMARY KEY (trial_id, intervention_id);


--
-- Name: trials_locations_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_locations
    ADD CONSTRAINT trials_locations_pkey PRIMARY KEY (trial_id, location_id);


--
-- Name: trials_organizations_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_organizations
    ADD CONSTRAINT trials_organizations_pkey PRIMARY KEY (trial_id, organisation_id);


--
-- Name: trials_persons_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_persons
    ADD CONSTRAINT trials_persons_pkey PRIMARY KEY (trial_id, person_id);


--
-- Name: trials_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials
    ADD CONSTRAINT trials_pkey PRIMARY KEY (id);


--
-- Name: trials_problems_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_conditions
    ADD CONSTRAINT trials_problems_pkey PRIMARY KEY (trial_id, condition_id);


--
-- Name: trials_publications_pkey; Type: CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_publications
    ADD CONSTRAINT trials_publications_pkey PRIMARY KEY (trial_id, publication_id);


--
-- Name: fda_approvals_type_index; Type: INDEX; Schema: public; Owner: database
--

CREATE INDEX fda_approvals_type_index ON fda_approvals USING btree (type);


--
-- Name: interventions_fda_application_number_index; Type: INDEX; Schema: public; Owner: database
--

CREATE INDEX interventions_fda_application_number_index ON interventions USING btree (fda_application_id);


--
-- Name: records_identifiers_index; Type: INDEX; Schema: public; Owner: database
--

CREATE INDEX records_identifiers_index ON records USING gin (identifiers);


--
-- Name: trialrecords_trial_id_index; Type: INDEX; Schema: public; Owner: database
--

CREATE INDEX trialrecords_trial_id_index ON records USING btree (trial_id);


--
-- Name: trials_documents_document_id_index; Type: INDEX; Schema: public; Owner: database
--

CREATE INDEX trials_documents_document_id_index ON trials_documents USING btree (document_id);


--
-- Name: conditions_set_updated_at; Type: TRIGGER; Schema: public; Owner: database
--

CREATE TRIGGER conditions_set_updated_at BEFORE UPDATE ON conditions FOR EACH ROW EXECUTE PROCEDURE set_updated_at();


--
-- Name: fda_applications_set_updated_at; Type: TRIGGER; Schema: public; Owner: database
--

CREATE TRIGGER fda_applications_set_updated_at BEFORE UPDATE ON fda_applications FOR EACH ROW EXECUTE PROCEDURE set_updated_at();


--
-- Name: fda_approvals_set_updated_at; Type: TRIGGER; Schema: public; Owner: database
--

CREATE TRIGGER fda_approvals_set_updated_at BEFORE UPDATE ON fda_approvals FOR EACH ROW EXECUTE PROCEDURE set_updated_at();


--
-- Name: interventions_set_updated_at; Type: TRIGGER; Schema: public; Owner: database
--

CREATE TRIGGER interventions_set_updated_at BEFORE UPDATE ON interventions FOR EACH ROW EXECUTE PROCEDURE set_updated_at();


--
-- Name: locations_set_updated_at; Type: TRIGGER; Schema: public; Owner: database
--

CREATE TRIGGER locations_set_updated_at BEFORE UPDATE ON locations FOR EACH ROW EXECUTE PROCEDURE set_updated_at();


--
-- Name: organizations_set_updated_at; Type: TRIGGER; Schema: public; Owner: database
--

CREATE TRIGGER organizations_set_updated_at BEFORE UPDATE ON organizations FOR EACH ROW EXECUTE PROCEDURE set_updated_at();


--
-- Name: persons_set_updated_at; Type: TRIGGER; Schema: public; Owner: database
--

CREATE TRIGGER persons_set_updated_at BEFORE UPDATE ON persons FOR EACH ROW EXECUTE PROCEDURE set_updated_at();


--
-- Name: publications_set_updated_at; Type: TRIGGER; Schema: public; Owner: database
--

CREATE TRIGGER publications_set_updated_at BEFORE UPDATE ON publications FOR EACH ROW EXECUTE PROCEDURE set_updated_at();


--
-- Name: records_set_updated_at; Type: TRIGGER; Schema: public; Owner: database
--

CREATE TRIGGER records_set_updated_at BEFORE UPDATE ON records FOR EACH ROW EXECUTE PROCEDURE set_updated_at();


--
-- Name: risk_of_bias_criteria_set_updated_at; Type: TRIGGER; Schema: public; Owner: database
--

CREATE TRIGGER risk_of_bias_criteria_set_updated_at BEFORE UPDATE ON risk_of_bias_criteria FOR EACH ROW EXECUTE PROCEDURE set_updated_at();


--
-- Name: risk_of_biases_set_updated_at; Type: TRIGGER; Schema: public; Owner: database
--

CREATE TRIGGER risk_of_biases_set_updated_at BEFORE UPDATE ON risk_of_biases FOR EACH ROW EXECUTE PROCEDURE set_updated_at();


--
-- Name: sources_set_updated_at; Type: TRIGGER; Schema: public; Owner: database
--

CREATE TRIGGER sources_set_updated_at BEFORE UPDATE ON sources FOR EACH ROW EXECUTE PROCEDURE set_updated_at();


--
-- Name: trials_set_updated_at; Type: TRIGGER; Schema: public; Owner: database
--

CREATE TRIGGER trials_set_updated_at BEFORE UPDATE ON trials FOR EACH ROW EXECUTE PROCEDURE set_updated_at();


--
-- Name: documents_document_category_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY documents
    ADD CONSTRAINT documents_document_category_id_foreign FOREIGN KEY (document_category_id) REFERENCES document_categories(id);


--
-- Name: documents_fda_approval_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY documents
    ADD CONSTRAINT documents_fda_approval_id_foreign FOREIGN KEY (fda_approval_id) REFERENCES fda_approvals(id);


--
-- Name: documents_file_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY documents
    ADD CONSTRAINT documents_file_id_foreign FOREIGN KEY (file_id) REFERENCES files(id);


--
-- Name: documents_source_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY documents
    ADD CONSTRAINT documents_source_id_foreign FOREIGN KEY (source_id) REFERENCES sources(id) ON UPDATE CASCADE;


--
-- Name: fda_applications_organisation_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY fda_applications
    ADD CONSTRAINT fda_applications_organisation_id_foreign FOREIGN KEY (organisation_id) REFERENCES organizations(id);


--
-- Name: fda_approvals_fda_application_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY fda_approvals
    ADD CONSTRAINT fda_approvals_fda_application_id_foreign FOREIGN KEY (fda_application_id) REFERENCES fda_applications(id) ON UPDATE CASCADE;


--
-- Name: interventions_fda_application_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY interventions
    ADD CONSTRAINT interventions_fda_application_id_foreign FOREIGN KEY (fda_application_id) REFERENCES fda_applications(id) ON UPDATE CASCADE;


--
-- Name: interventions_source_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY interventions
    ADD CONSTRAINT interventions_source_id_foreign FOREIGN KEY (source_id) REFERENCES sources(id) ON UPDATE CASCADE;


--
-- Name: locations_source_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY locations
    ADD CONSTRAINT locations_source_id_foreign FOREIGN KEY (source_id) REFERENCES sources(id) ON UPDATE CASCADE;


--
-- Name: organizations_source_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY organizations
    ADD CONSTRAINT organizations_source_id_foreign FOREIGN KEY (source_id) REFERENCES sources(id) ON UPDATE CASCADE;


--
-- Name: persons_source_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY persons
    ADD CONSTRAINT persons_source_id_foreign FOREIGN KEY (source_id) REFERENCES sources(id) ON UPDATE CASCADE;


--
-- Name: problems_source_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY conditions
    ADD CONSTRAINT problems_source_id_foreign FOREIGN KEY (source_id) REFERENCES sources(id) ON UPDATE CASCADE;


--
-- Name: publications_source_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY publications
    ADD CONSTRAINT publications_source_id_foreign FOREIGN KEY (source_id) REFERENCES sources(id) ON UPDATE CASCADE;


--
-- Name: risk_of_biases_risk_of_bias_criteria_risk_of_bias_criteria_id_; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY risk_of_biases_risk_of_bias_criteria
    ADD CONSTRAINT risk_of_biases_risk_of_bias_criteria_risk_of_bias_criteria_id_ FOREIGN KEY (risk_of_bias_criteria_id) REFERENCES risk_of_bias_criteria(id);


--
-- Name: risk_of_biases_risk_of_bias_criteria_risk_of_bias_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY risk_of_biases_risk_of_bias_criteria
    ADD CONSTRAINT risk_of_biases_risk_of_bias_criteria_risk_of_bias_id_foreign FOREIGN KEY (risk_of_bias_id) REFERENCES risk_of_biases(id);


--
-- Name: risk_of_biases_source_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY risk_of_biases
    ADD CONSTRAINT risk_of_biases_source_id_foreign FOREIGN KEY (source_id) REFERENCES sources(id);


--
-- Name: risk_of_biases_trial_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY risk_of_biases
    ADD CONSTRAINT risk_of_biases_trial_id_foreign FOREIGN KEY (trial_id) REFERENCES trials(id);


--
-- Name: trialrecords_source_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY records
    ADD CONSTRAINT trialrecords_source_id_foreign FOREIGN KEY (source_id) REFERENCES sources(id) ON UPDATE CASCADE;


--
-- Name: trialrecords_trial_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY records
    ADD CONSTRAINT trialrecords_trial_id_foreign FOREIGN KEY (trial_id) REFERENCES trials(id);


--
-- Name: trials_conditions_trial_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_conditions
    ADD CONSTRAINT trials_conditions_trial_id_foreign FOREIGN KEY (trial_id) REFERENCES trials(id) ON DELETE CASCADE;


--
-- Name: trials_documents_document_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_documents
    ADD CONSTRAINT trials_documents_document_id_foreign FOREIGN KEY (document_id) REFERENCES documents(id);


--
-- Name: trials_documents_trial_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_documents
    ADD CONSTRAINT trials_documents_trial_id_foreign FOREIGN KEY (trial_id) REFERENCES trials(id);


--
-- Name: trials_interventions_intervention_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_interventions
    ADD CONSTRAINT trials_interventions_intervention_id_foreign FOREIGN KEY (intervention_id) REFERENCES interventions(id);


--
-- Name: trials_interventions_trial_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_interventions
    ADD CONSTRAINT trials_interventions_trial_id_foreign FOREIGN KEY (trial_id) REFERENCES trials(id) ON DELETE CASCADE;


--
-- Name: trials_locations_location_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_locations
    ADD CONSTRAINT trials_locations_location_id_foreign FOREIGN KEY (location_id) REFERENCES locations(id);


--
-- Name: trials_locations_trial_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_locations
    ADD CONSTRAINT trials_locations_trial_id_foreign FOREIGN KEY (trial_id) REFERENCES trials(id) ON DELETE CASCADE;


--
-- Name: trials_organizations_organisation_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_organizations
    ADD CONSTRAINT trials_organizations_organisation_id_foreign FOREIGN KEY (organisation_id) REFERENCES organizations(id);


--
-- Name: trials_organizations_trial_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_organizations
    ADD CONSTRAINT trials_organizations_trial_id_foreign FOREIGN KEY (trial_id) REFERENCES trials(id) ON DELETE CASCADE;


--
-- Name: trials_persons_person_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_persons
    ADD CONSTRAINT trials_persons_person_id_foreign FOREIGN KEY (person_id) REFERENCES persons(id);


--
-- Name: trials_persons_trial_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_persons
    ADD CONSTRAINT trials_persons_trial_id_foreign FOREIGN KEY (trial_id) REFERENCES trials(id) ON DELETE CASCADE;


--
-- Name: trials_problems_problem_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_conditions
    ADD CONSTRAINT trials_problems_problem_id_foreign FOREIGN KEY (condition_id) REFERENCES conditions(id);


--
-- Name: trials_publications_publication_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_publications
    ADD CONSTRAINT trials_publications_publication_id_foreign FOREIGN KEY (publication_id) REFERENCES publications(id);


--
-- Name: trials_publications_trial_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials_publications
    ADD CONSTRAINT trials_publications_trial_id_foreign FOREIGN KEY (trial_id) REFERENCES trials(id) ON DELETE CASCADE;


--
-- Name: trials_source_id_foreign; Type: FK CONSTRAINT; Schema: public; Owner: database
--

ALTER TABLE ONLY trials
    ADD CONSTRAINT trials_source_id_foreign FOREIGN KEY (source_id) REFERENCES sources(id) ON UPDATE CASCADE;


--
-- Name: conditions; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE conditions FROM PUBLIC;
REVOKE ALL ON TABLE conditions FROM database;
GRANT ALL ON TABLE conditions TO database;
GRANT SELECT ON TABLE conditions TO opentrials_readonly;


--
-- Name: document_categories; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE document_categories FROM PUBLIC;
REVOKE ALL ON TABLE document_categories FROM database;
GRANT ALL ON TABLE document_categories TO database;
GRANT SELECT ON TABLE document_categories TO opentrials_readonly;


--
-- Name: documents; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE documents FROM PUBLIC;
REVOKE ALL ON TABLE documents FROM database;
GRANT ALL ON TABLE documents TO database;
GRANT SELECT ON TABLE documents TO opentrials_readonly;


--
-- Name: fda_applications; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE fda_applications FROM PUBLIC;
REVOKE ALL ON TABLE fda_applications FROM database;
GRANT ALL ON TABLE fda_applications TO database;
GRANT SELECT ON TABLE fda_applications TO opentrials_readonly;


--
-- Name: fda_approvals; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE fda_approvals FROM PUBLIC;
REVOKE ALL ON TABLE fda_approvals FROM database;
GRANT ALL ON TABLE fda_approvals TO database;
GRANT SELECT ON TABLE fda_approvals TO opentrials_readonly;


--
-- Name: files; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE files FROM PUBLIC;
REVOKE ALL ON TABLE files FROM database;
GRANT ALL ON TABLE files TO database;
GRANT SELECT ON TABLE files TO opentrials_readonly;


--
-- Name: interventions; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE interventions FROM PUBLIC;
REVOKE ALL ON TABLE interventions FROM database;
GRANT ALL ON TABLE interventions TO database;
GRANT SELECT ON TABLE interventions TO opentrials_readonly;


--
-- Name: knex_migrations; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE knex_migrations FROM PUBLIC;
REVOKE ALL ON TABLE knex_migrations FROM database;
GRANT ALL ON TABLE knex_migrations TO database;
GRANT SELECT ON TABLE knex_migrations TO opentrials_readonly;


--
-- Name: knex_migrations_id_seq; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON SEQUENCE knex_migrations_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE knex_migrations_id_seq FROM database;
GRANT ALL ON SEQUENCE knex_migrations_id_seq TO database;
GRANT SELECT ON SEQUENCE knex_migrations_id_seq TO opentrials_readonly;


--
-- Name: knex_migrations_lock; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE knex_migrations_lock FROM PUBLIC;
REVOKE ALL ON TABLE knex_migrations_lock FROM database;
GRANT ALL ON TABLE knex_migrations_lock TO database;
GRANT SELECT ON TABLE knex_migrations_lock TO opentrials_readonly;


--
-- Name: locations; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE locations FROM PUBLIC;
REVOKE ALL ON TABLE locations FROM database;
GRANT ALL ON TABLE locations TO database;
GRANT SELECT ON TABLE locations TO opentrials_readonly;


--
-- Name: organizations; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE organizations FROM PUBLIC;
REVOKE ALL ON TABLE organizations FROM database;
GRANT ALL ON TABLE organizations TO database;
GRANT SELECT ON TABLE organizations TO opentrials_readonly;


--
-- Name: persons; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE persons FROM PUBLIC;
REVOKE ALL ON TABLE persons FROM database;
GRANT ALL ON TABLE persons TO database;
GRANT SELECT ON TABLE persons TO opentrials_readonly;


--
-- Name: publications; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE publications FROM PUBLIC;
REVOKE ALL ON TABLE publications FROM database;
GRANT ALL ON TABLE publications TO database;
GRANT SELECT ON TABLE publications TO opentrials_readonly;


--
-- Name: records; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE records FROM PUBLIC;
REVOKE ALL ON TABLE records FROM database;
GRANT ALL ON TABLE records TO database;
GRANT SELECT ON TABLE records TO opentrials_readonly;


--
-- Name: risk_of_bias_criteria; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE risk_of_bias_criteria FROM PUBLIC;
REVOKE ALL ON TABLE risk_of_bias_criteria FROM database;
GRANT ALL ON TABLE risk_of_bias_criteria TO database;
GRANT SELECT ON TABLE risk_of_bias_criteria TO opentrials_readonly;


--
-- Name: risk_of_biases; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE risk_of_biases FROM PUBLIC;
REVOKE ALL ON TABLE risk_of_biases FROM database;
GRANT ALL ON TABLE risk_of_biases TO database;
GRANT SELECT ON TABLE risk_of_biases TO opentrials_readonly;


--
-- Name: risk_of_biases_risk_of_bias_criteria; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE risk_of_biases_risk_of_bias_criteria FROM PUBLIC;
REVOKE ALL ON TABLE risk_of_biases_risk_of_bias_criteria FROM database;
GRANT ALL ON TABLE risk_of_biases_risk_of_bias_criteria TO database;
GRANT SELECT ON TABLE risk_of_biases_risk_of_bias_criteria TO opentrials_readonly;


--
-- Name: sources; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE sources FROM PUBLIC;
REVOKE ALL ON TABLE sources FROM database;
GRANT ALL ON TABLE sources TO database;
GRANT SELECT ON TABLE sources TO opentrials_readonly;


--
-- Name: trials; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE trials FROM PUBLIC;
REVOKE ALL ON TABLE trials FROM database;
GRANT ALL ON TABLE trials TO database;
GRANT SELECT ON TABLE trials TO opentrials_readonly;


--
-- Name: trials_conditions; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE trials_conditions FROM PUBLIC;
REVOKE ALL ON TABLE trials_conditions FROM database;
GRANT ALL ON TABLE trials_conditions TO database;
GRANT SELECT ON TABLE trials_conditions TO opentrials_readonly;


--
-- Name: trials_documents; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE trials_documents FROM PUBLIC;
REVOKE ALL ON TABLE trials_documents FROM database;
GRANT ALL ON TABLE trials_documents TO database;
GRANT SELECT ON TABLE trials_documents TO opentrials_readonly;


--
-- Name: trials_interventions; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE trials_interventions FROM PUBLIC;
REVOKE ALL ON TABLE trials_interventions FROM database;
GRANT ALL ON TABLE trials_interventions TO database;
GRANT SELECT ON TABLE trials_interventions TO opentrials_readonly;


--
-- Name: trials_locations; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE trials_locations FROM PUBLIC;
REVOKE ALL ON TABLE trials_locations FROM database;
GRANT ALL ON TABLE trials_locations TO database;
GRANT SELECT ON TABLE trials_locations TO opentrials_readonly;


--
-- Name: trials_organizations; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE trials_organizations FROM PUBLIC;
REVOKE ALL ON TABLE trials_organizations FROM database;
GRANT ALL ON TABLE trials_organizations TO database;
GRANT SELECT ON TABLE trials_organizations TO opentrials_readonly;


--
-- Name: trials_persons; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE trials_persons FROM PUBLIC;
REVOKE ALL ON TABLE trials_persons FROM database;
GRANT ALL ON TABLE trials_persons TO database;
GRANT SELECT ON TABLE trials_persons TO opentrials_readonly;


--
-- Name: trials_publications; Type: ACL; Schema: public; Owner: database
--

REVOKE ALL ON TABLE trials_publications FROM PUBLIC;
REVOKE ALL ON TABLE trials_publications FROM database;
GRANT ALL ON TABLE trials_publications TO database;
GRANT SELECT ON TABLE trials_publications TO opentrials_readonly;


--
-- PostgreSQL database dump complete
--


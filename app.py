import ast
import json
import time

import pandas as pd
import streamlit as st
import xmltodict
from dotenv import load_dotenv

from services import DataHarmonizationService

load_dotenv()

st.set_page_config(layout="wide")


def initialize_session_state():
    for key in [
        "df_keymap",
        "df_data",
        "keymap_id",
        "data_id",
        "provider_name",
        "df_data_schema",
    ]:
        if key not in st.session_state:
            st.session_state[key] = None


def get_service():
    try:
        return DataHarmonizationService()
    except Exception as e:
        st.error(f"Failed to initialize database connection: {str(e)}")
        return None


def safe_json_loads(val):
    if isinstance(val, dict) or isinstance(val, list) or val is None:
        return val
    try:
        return json.loads(val)
    except Exception:
        try:
            return ast.literal_eval(val)
        except Exception:
            return val


def fix_json_columns(records, columns_to_fix):
    for row in records:
        for col in columns_to_fix:
            if col in row:
                row[col] = safe_json_loads(row[col])
    return records


def execute_final_workflow(service, input_data_dict, target_schema_id, provider_name):
    result = service.final_workflow(target_schema_id, provider_name, input_data_dict)
    if result.get("success"):
        file_data = result.get("data")
        # Prepare DataFrame
        if isinstance(file_data, list):
            df = pd.DataFrame(file_data)
        elif isinstance(file_data, dict):
            try:
                df = pd.DataFrame(file_data)
            except Exception:
                df = pd.DataFrame([file_data])
        else:
            st.warning("Returned file data is not a recognized JSON structure.")
            return None, None
        json_str = json.dumps(file_data, indent=2)
        return df, json_str
    else:
        st.error(f"Workflow execution failed: {result.get('error', 'Unknown error')}")
        return None, None


def handle_form_submission(
    service,
    provider_name,
    data_domain,
    source_schema_version,
    target_schema_version,
    generate_missing_key,
    input_file,
):
    form_data = {
        "provider_name": provider_name,
        "data_domain": data_domain,
        "source_schema_version": source_schema_version,
        "target_schema_version": target_schema_version,
        "generate_missing_key": str(generate_missing_key).lower(),
    }

    result = service.submit_harmonization_request(form_data, input_file)

    if (
        result
        and result.get("status_code") == 200
        and "_err" not in result["response_data"][0].get("schemaVersion", "")
    ):
        docs = result["response_data"]
        if not docs:
            st.error("No documents found for the target schema version.")
            return
        doc = docs[0]  # Use the first document
        st.success("KeyMap and Master Schema successfully generated!")
        keymap_data = service.fetch_keymap_data(provider_name)
        if isinstance(keymap_data, str):
            st.error("Something went wrong while fetching keymap data")
            return

        df_keymap = pd.DataFrame(
            keymap_data[0][provider_name].items(), columns=["Source", "Target"]
        )
        df_data = pd.DataFrame(doc["schema"])
        df_data_schema = pd.DataFrame(
            [
                {
                    "schemaVersion": doc["schemaVersion"],
                    "_id": doc["_id"],
                    "statusFlow": doc.get("statusFlow", "NA"),
                }
            ]
        )
        st.session_state.df_keymap = df_keymap
        st.session_state.df_data = df_data
        st.session_state.keymap_id = keymap_data[0]["_id"]
        st.session_state.data_id = doc["_id"]
        st.session_state.df_data_schema = df_data_schema

    else:  # Error Scenario
        print("Error Scenario")
        keymap_data = service.fetch_keymap_data(provider_name)

        if isinstance(keymap_data, str):
            st.error(keymap_data)
        else:
            df_keymap = pd.DataFrame(
                keymap_data[0][provider_name].items(), columns=["Source", "Target"]
            )

            st.session_state.df_keymap = df_keymap
            st.session_state.keymap_id = keymap_data[0]["_id"]
        time.sleep(10)
        target_schema_data = service.fetch_data_from_target_schema(
            target_schema_version + "_err"
        )
        if isinstance(target_schema_data, str):
            return st.error(target_schema_data)

        df_data = pd.DataFrame(target_schema_data[0]["schema"])
        df_data_schema = pd.DataFrame(
            [
                {
                    "schemaVersion": target_schema_data[0]["schemaVersion"],
                    "_id": target_schema_data[0]["_id"],
                    "statusFlow": target_schema_data[0].get("statusFlow", "NA"),
                }
            ]
        )

        st.session_state.df_data = df_data

        st.session_state.data_id = target_schema_data[0]["_id"]
        st.session_state.df_data_schema = df_data_schema


def show_final_workflow_result(provider_name):
    if (
        st.session_state.get("final_workflow_df") is not None
        and st.session_state.get("final_workflow_json") is not None
    ):
        st.markdown("---")
        st.markdown("### Final Workflow Result")
        st.dataframe(st.session_state.final_workflow_df, use_container_width=True)
        st.download_button(
            label="Download JSON file",
            data=st.session_state.final_workflow_json,
            file_name=f"{provider_name}.json",
            mime="application/json",
        )


def show_missing_keys(service, target_schema_version):
    st.markdown("---")
    st.markdown("### Missing Keys")
    missing_keys_data = service.fetch_missing_keys_data(target_schema_version)
    st.dataframe(missing_keys_data, use_container_width=True)


def show_editors_and_update(service, provider_name, target_schema_version):
    st.markdown("---")
    st.markdown("### GeneratedSchema Info")
    st.table(st.session_state.df_data_schema)

    # Initialize or increment editor version for forcing re-render
    if "editor_version" not in st.session_state:
        st.session_state.editor_version = 0

    st.markdown("## Edit Keymap & Target Schema")

    # Create two columns for side-by-side display
    col1, col2 = st.columns([0.7, 0.3])

    with col1:
        st.write("### Keymap")
        edited_keymap = st.data_editor(
            st.session_state.df_keymap,
            key="keymap_editor",
            num_rows="dynamic",
            on_change=lambda: st.session_state.df_keymap,
        )

    with col2:
        # Show Missing Keys table when generate_missing_key is true
        if st.session_state.get("generate_missing_key", False):
            st.write("### Missing Keys")
            missing_keys_data = service.fetch_missing_keys_data(target_schema_version)
            if isinstance(missing_keys_data, str):
                st.error(missing_keys_data)
            else:
                st.dataframe(missing_keys_data, use_container_width=True)

    # Target schema section below the columns
    st.write("### Target schema")
    edited_schema = st.data_editor(
        st.session_state.df_data,
        key="schema_editor",
        num_rows="dynamic",
        on_change=lambda: st.session_state.df_data,
    )

    updated_schema = fix_json_columns(
        edited_schema.to_dict("records"), ["constraints", "itemDefinition"]
    )

    with st.form("edit_form", clear_on_submit=False, border=False):
        if st.form_submit_button(
            "Update in MongoDB",
            type="primary",
            disabled=st.session_state.get("df_keymap") is None,
        ):
            ok = service.update_collections_data(
                data_id=st.session_state.data_id,
                updated_schema=updated_schema,
                keymap_id=st.session_state.keymap_id,
                updated_keymap=dict(edited_keymap.values),
                provider_name=provider_name,
            )

            if ok:
                st.success("Edits saved to MongoDB!")
                # Execute the next workflow and store result in session state
                if (
                    "input_file_data" in st.session_state
                    and st.session_state.input_file_data is not None
                ):
                    df_result, json_result = execute_final_workflow(
                        service,
                        st.session_state.input_file_data,
                        st.session_state.data_id,
                        provider_name,
                    )
                    st.session_state.final_workflow_df = df_result
                    st.session_state.final_workflow_json = json_result
            else:  # Error Scenario
                print("Error Scenario")
                keymap_data = service.fetch_keymap_data(provider_name)

                if isinstance(keymap_data, str):
                    st.error(keymap_data)
                else:
                    df_keymap = pd.DataFrame(
                        keymap_data[0][provider_name].items(),
                        columns=["Source", "Target"],
                    )
                    st.session_state.df_keymap = df_keymap
                    st.session_state.keymap_id = keymap_data[0]["_id"]
                    time.sleep(10)
                    target_schema_data = service.fetch_data_from_target_schema(
                        target_schema_version + "_err"
                    )
                    if isinstance(target_schema_data, str):
                        return st.error(target_schema_data)

                    df_data = pd.DataFrame(target_schema_data[0]["schema"])
                    df_data_schema = pd.DataFrame(
                        [
                            {
                                "schemaVersion": target_schema_data[0]["schemaVersion"],
                                "_id": target_schema_data[0]["_id"],
                                "statusFlow": target_schema_data[0].get(
                                    "statusFlow", "NA"
                                ),
                            }
                        ]
                    )

                    st.session_state.df_data = df_data

                    st.session_state.data_id = target_schema_data[0]["_id"]
                    st.session_state.df_data_schema = df_data_schema


def main():
    initialize_session_state()
    service = get_service()
    if not service:
        st.error("Could not connect to the database. Please check your configuration.")
        return

    st.title("Data Harmonization")

    with st.form("schema_config_form"):
        initialize_session_state()
        col1, col2 = st.columns(2)
        with col1:
            provider_name = st.text_input("Provider Name", key="provider_name")
            schema_versions = service.get_schema_versions()
            source_schema_version = st.selectbox(
                "Source Schema Version",
                options=schema_versions
                if schema_versions
                else ["No versions available"],
                key="source_schema_version",
            )
        with col2:
            data_domain = st.text_input("Data Domain", key="data_domain")
            target_schema_version = st.text_input(
                "Target Schema Version", key="target_schema_version"
            )
        generate_missing_key = st.checkbox(
            "Generate Missing Key", key="generate_missing_key"
        )
        initialize_session_state()
        input_file = st.file_uploader(
            "Upload Input File", type=["json", "xml"], key="input_file"
        )
        submit_button = st.form_submit_button("Submit")

    if submit_button:
        if input_file is not None:
            with st.spinner("Processing your request..."):
                # try:
                handle_form_submission(
                    service,
                    provider_name,
                    data_domain,
                    source_schema_version,
                    target_schema_version,
                    generate_missing_key,
                    input_file,
                )
                input_file.seek(0)
                if input_file.name.endswith(".json"):
                    input_file_dict = json.load(input_file)
                elif input_file.name.endswith(".xml"):
                    input_file_dict = xmltodict.parse(input_file.read())
                    # Remove the root key if present (In case of xml)
                    if isinstance(input_file_dict, dict) and len(input_file_dict) == 1:
                        input_file_dict = next(iter(input_file_dict.values()))
                else:
                    st.error("Unsupported file type")
                    return
                st.session_state.input_file_data = input_file_dict
        else:
            st.error("Please upload an input file")

    if st.session_state.get("df_data") is not None:
        show_editors_and_update(service, provider_name, target_schema_version)
        show_final_workflow_result(provider_name)


if __name__ == "__main__":
    main()

import ast
import json
import time

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from services import DataHarmonizationService

load_dotenv()


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

    if result:
        st.success("KeyMap and Master Schema successfully generated!")
        keymap_data = service.fetch_keymap_data(provider_name)
        target_schema_data = service.fetch_data_from_target_schema(
            target_schema_version
        )
        df_keymap = pd.DataFrame(
            keymap_data[0][provider_name].items(), columns=["Source", "Target"]
        )
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
        st.session_state.df_keymap = df_keymap
        st.session_state.df_data = df_data
        st.session_state.keymap_id = keymap_data[0]["_id"]
        st.session_state.data_id = target_schema_data[0]["_id"]
        st.session_state.df_data_schema = df_data_schema

    else:  # Error Scenario
        print("Error Scenario")
        keymap_data = service.fetch_keymap_data(provider_name)
        print("keymap_data::", keymap_data)
        if isinstance(keymap_data, str):
            st.error(keymap_data)
        else:
            df_keymap = pd.DataFrame(
                keymap_data[0][provider_name].items(), columns=["Source", "Target"]
            )
            print("df_keymap::", df_keymap)
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


def show_editors_and_update(service, provider_name):
    st.markdown("---")
    st.markdown("### GeneratedSchema Info")
    st.table(st.session_state.df_data_schema)

    st.markdown("## Edit Keymap & Target Schema")
    with st.form("edit_form", clear_on_submit=False):
        st.write("### Keymap")
        edited_keymap = st.data_editor(st.session_state.df_keymap, key="keymap_editor")
        st.session_state.df_keymap = edited_keymap

        st.write("### Target schema")
        edited_schema = st.data_editor(st.session_state.df_data, key="schema_editor")
        st.session_state.df_data = edited_schema

        updated_schema = fix_json_columns(
            edited_schema.to_dict("records"), ["constraints", "itemDefinition"]
        )

        if st.form_submit_button(
            "Update in MongoDB",
            type="primary",
            disabled=st.session_state.get("df_keymap").empty,
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
                        st.session_state.input_file_data,  # This is now a dict, not a file
                        st.session_state.data_id,
                        provider_name,
                    )
                    st.session_state.final_workflow_df = df_result
                    st.session_state.final_workflow_json = json_result
            else:
                st.error("Failed to update one or both collections in MongoDB.")


def main():
    service = get_service()
    if not service:
        st.error("Could not connect to the database. Please check your configuration.")
        return

    initialize_session_state()
    st.title("Data Harmonization")

    with st.form("schema_config_form"):
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
        input_file = st.file_uploader(
            "Upload Input File", type=["json"], key="input_file"
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
                input_file_dict = json.load(input_file)
                st.session_state.input_file_data = input_file_dict
        # except Exception as e:
        #     st.error(f"An error occurred: {str(e)}")
        else:
            st.error("Please upload an input file")

    if st.session_state.get("df_data") is not None:
        show_editors_and_update(service, provider_name)
        show_final_workflow_result(provider_name)


if __name__ == "__main__":
    main()

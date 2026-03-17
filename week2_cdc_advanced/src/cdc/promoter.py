def promote_run(fs, base_path, run_id):
    staging_path = f"base_path/_staging/run_id={run_id}"

    partitions = fs.ls(staging_path)

    for partition_path in partitions:
        partition_name = partition_path.split("/")[-1]
        print("staging_path", partition_name)
        final_partition = f"{base_path}/{partition_name}"

        files = fs.ls(partition_path)
        print("final_partiton", final_partition)

        for file in files:
            print(file)
            filename = file.split("/")[-1]

            target = f"{final_partition}/{filename}"

            fs.mv(file, target)

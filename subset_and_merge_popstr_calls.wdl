version 1.0

task get_batch_files {
  input {
    File popstr_files_list
    Int batch
    Int batch_size
    Int idx
  }

  command <<<
    head -n $(((~{batch} + 1)  * ~{batch_size})) ~{popstr_files_list} | tail -n ~{batch_size} | awk '{print $~{idx}}'
  >>>

  output {
    Array[File] files = prefix(
      'dx://UKB_GymrekLab_v2:',
      read_lines(stdout())
    )
  }

  runtime {
    dx_timeout: "30m"
    memory: "2GB"
    docker: "quay.io/thedevilinthedetails/work/bcftools_trtools_dxpy:v1.0"
  }
}

task subset_batch {
  input {
    Array[File] batch_vcfs_list
    Array[File] batch_idxs_list
    File my_calls_bed
    String suffix 
  }

  command <<<
    echo 'Running'
    mkdir subsetted_calls

    for file in ~{sep=" " batch_vcfs_list} ; do
      echo $file
      #envsetup dx download 'Bulk/Previous WGS releases/GATK and GraphTyper WGS/Microsatellites [150k release]/$file' 
      envsetup bcftools view -R ~{my_calls_bed} $file > subsetted_calls/$(echo $file | sed -e 's/\.gz$//')
    done 

    envsetup bcftools concat subsetted_calls/* -o subsetted_calls_~{suffix}.vcf.gz -O z
  >>>

  output {
    File out = "subsetted_calls_~{suffix}.vcf.gz"
  }

  runtime {
    dx_timeout: "24h"
    memory: "2GB"
    docker: "get_batch_filesquay.io/thedevilinthedetails/work/bcftools_trtools_dxpy:v1.0"
  }
}

task concat_batches {
  input {
    Array[File] subsetted_calls_batches
  }

  command <<<
    envsetup bcftools concat ~{sep=" " subsetted_calls_batches} -o subsetted_calls.vcf.gz -O z
  >>>

  output {
    File out = "subsetted_calls.vcf.gz"
  }

  runtime {
    dx_timeout: "24h"
    memory: "2GB"
    docker: "quay.io/thedevilinthedetails/work/bcftools_trtools_dxpy:v1.0"
  }
}

workflow subset_all {
  input {
    File popstr_files_list = 'dx://UKB_GymrekLab_v2:/popstr_comparison/popstr_file_ids.txt'
    File my_calls_bed = 'dx://UKB_GymrekLab_v2:/popstr_comparison/Margoliash_paper_str_calls_20230907.vcf.gz'
  }

  # n files = len(popstr_files_list)/2 == 54182
  # thus 109 batches at 500 files per batch

  #scatter (batch in range(109)) {
  scatter (batch in range(1)) {
    call get_batch_files as batch_vcf_files { input :
      popstr_files_list = popstr_files_list,
      batch = batch,
      #batch_size = 500
      batch_size = 10,
      idx = 1
    }

  call get_batch_files as batch_idx_files { input :
      popstr_files_list = popstr_files_list,
      batch = batch,
      #batch_size = 500
      batch_size = 10,
      idx = 2
    }

    call subset_batch { input : 
      batch_vcfs_list = batch_vcf_files.files,
      batch_idxs_list = batch_idx_files.files,
      my_calls_bed = my_calls_bed,
      suffix = "batch_~{batch}"
    }
  }

  call concat_batches { input :
    subsetted_calls_batches = subset_batch.out
  }

  output {
    File out = concat_batches.out
  }
}

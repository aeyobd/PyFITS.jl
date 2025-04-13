module PyFITS

export read_fits, write_fits


import DataFrames: DataFrame
using PythonCall



const AstroPyTable = Ref{Py}()
const Numpy = Ref{Py}()


function __init__()
    AstroPyTable[] = pyimport("astropy.table")
    Numpy[] = pyimport("numpy")
end


function pycol_to_vec(col)
    dtype = col.dtype
end


"""
    py_read_fits_table(filename; hdu=2)

Read the fits table as an astroy.table.Table object.
"""
function py_read_fits_table(filename::String; hdu::Int=2, kwargs...)
    tab = AstroPyTable[].Table.read(filename; hdu=hdu-1, format="fits", kwargs...)
    return tab
end


"""
    extract_table_data(py_table::Py)

Return the table data as a dictionary of Numpy arrays and a 
dictionary of masks
"""
function extract_table_data(py_table::Py, columns::Vector{String})
    data_dict = Dict{String, Py}()
    mask_dict = Dict{String, Py}()
    for colname in columns
        col = py_table[colname]
        data = col.data
        if pyhasattr(col, "mask")
            data = col.data.data
            mask = col.mask
            mask_dict[colname] = mask
        end

        data_dict[colname] = data
    end

    return data_dict, mask_dict
end


function convert_pycols(py_data_dict, py_mask_dict)
    colnames = keys(py_data_dict)

    data_dict = Dict{String, Any}()

    for colname in colnames
        py_data = py_data_dict[colname]
        @debug "column $colname"
        @debug "shape: $(py_data.shape)"
        @debug "dtype: $(py_data.dtype)"

        if pytruth(Numpy[].isdtype(py_data.dtype, Numpy[].bytes_))
            py_data = py_data.astype("U")
            data = pyconvert(Array{String}, py_data)
        else
            data = pyconvert(Array, py_data)
        end

        if colname ∈ keys(py_mask_dict)
            py_mask = py_mask_dict[colname]
            mask = pyconvert(Vector{Bool}, py_mask)

            outdata = Vector{Union{eltype(data), Missing}}(undef, length(data))
            outdata .= data
            outdata[mask] .= missing
        else
            outdata = data
        end

        data_dict[colname] = outdata
    end

    return data_dict
end



"""
    read_fits(filename; hdu=2)

Load a FITS file and return a DataFrame using the specified HDU (1-indexed).

NOTE. This function used to use FITSIO. However, this package 
still is not fully mature and has a tendency to segfault do to poor management
of c-pointers (not easy in julia).
Now, fits are read in using astropy.
"""
function read_fits(filename::String; hdu=2, columns=nothing, kwargs...)
    table = py_read_fits_table(filename; hdu=hdu, kwargs...)
    @info "astropy table opened"
    columns = check_columns(table, columns)

    py_data, py_mask = extract_table_data(table, columns)
    data = convert_pycols(py_data, py_mask)

    df = DataFrame()
    for (colname, coldat) in data
        df[!, colname] = coldat
    end

    return df
end



function check_columns(tab::Py, columns)
    all_columns = pyconvert(Vector{String}, tab.columns)

    if isnothing(columns)
        columns = all_columns
    else
        if !issubset(columns, all_columns)
            @error "Columns not found in data: $(setdiff(columns, all_columns))"
        end
    end

    return columns
end




"""
    write_fits(filename, dataframe; overwrite=false)

Write a DataFrame to a FITS file.
"""
function write_fits(filename::String, df::DataFrame;
        overwrite=false, 
    )

    check_colnames(df)
    # Convert DataFrame to Python dictionary of columns
    py_columns = PyDict{String,Py}()

    
    for name in names(df)
        col = df[!, name]
        # Convert column data to Python-friendly format
        @debug "converting $name"
        py_col = jlcol_to_py(col)
        py_columns[String(name)] = py_col
    end

    # Create Astropy Table
    py_table = AstroPyTable[].Table(py_columns)
    
    # Write to FITS file
    py_table.write(filename, format="fits", overwrite=overwrite)
    return
end


"""
    jlcol_to_py

Converts a julia column to a pandas column
"""
function jlcol_to_py(col::AbstractArray)
    # Handle missing values
    if any(ismissing, col)
        @debug "masking $col"
        return create_masked_array(col)
    end
    
    # Handle basic types
    if eltype(col) <: AbstractString
        return Numpy[].array(PyArray(col), dtype="U$(maximum(length, skipmissing(col)))")
    end
    
    # Use zero-copy conversion for numeric arrays
    return Numpy[].asarray(PyArray(col))
end


"""
    create_masked_array

Creates a masked array from the file.
"""
function create_masked_array(col::AbstractArray)
    # get filler type
    T = nonmissingtype(eltype(col))
    fill_val = if T <: Bool
        @warn "masked Bools not supported well by astropy"
        false
    elseif T <: AbstractFloat
        T(NaN)
    elseif T <: Integer
        typemin(T)
    elseif T <: AbstractString
        ""
    else
        error("Unsupported column type: $T")
    end

    mask = ismissing.(col)
    data = Vector{T}(undef, length(col))
    
    for (i, val) in enumerate(col)
        if mask[i]
            data[i] = fill_val
        else
            data[i] = val
        end
    end

    if T  <: AbstractString
        return Numpy[].ma.MaskedArray(
            Numpy[].array(PyArray(data), dtype="U$(maximum(length, data))"),
            mask = Numpy[].array(mask)
       )
    else
        return Numpy[].ma.MaskedArray(
            Numpy[].array(data),
            mask=Numpy[].array(mask),
        )
    end
end


function check_colnames(df)
    try 
        column_names = ascii.(names(df))
    catch e
        if isa(e, ArgumentError)
            throw(ArgumentError("Column names must be ASCII"))
        else
            rethrow(e)
        end
    end
end


end # module PyFITS
